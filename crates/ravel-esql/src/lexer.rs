//! Command-aware tokenizer for ES|QL statement text.
//!
//! Text after `FROM` or `TS` is a list of source patterns (`logs-prod-*`,
//! `cluster:index`, `"quoted"`), the policy of `ENRICH` and the index of
//! `LOOKUP JOIN` are one such pattern, and the name lists of `KEEP` and
//! `DROP` may contain `*` wildcards; none of them lexes like an expression.
//! The lexer therefore tracks which command it is inside: the first word
//! after the start of the statement, a `|` or a `;` selects the mode for the
//! rest of that command, and `METADATA` ends the source list of a `FROM`. A
//! source name is expected first and after each comma, so a source named
//! `metadata` in either place lexes as a source, not as that keyword.
//!
//! A parenthesis that opens a nested pipeline (a `FORK` branch, a subquery
//! among the sources of `FROM`, or `IN (FROM ...)`) starts a command, and the
//! matching `)` restores the mode of the command around it. The query of
//! `PROMQL` is one token: from its `(` to the matching `)`, skipping PromQL
//! strings and `#` comments, never split on `|`.
//!
//! Whitespace, `//` line comments and `/* */` block comments (which nest)
//! separate tokens and produce none. Keywords are not distinguished here: a
//! keyword is a [`TokenKind::Word`] whose text the parser compares without
//! regard to case, and the token's span keeps the original spelling.

use crate::ast::ParamName;
use crate::error::Error;
use crate::limits::{LimitKind, Limits};
use crate::span::Span;

/// One token and the bytes it was lexed from. Only [`lex`] makes one, so a
/// token outside this crate always came through the byte and token limits.
///
/// ```compile_fail
/// use ravel_esql::{Span, Token, TokenKind};
/// let token = Token { kind: TokenKind::Pipe, span: Span::new(0, 1) };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) span: Span,
}

impl Token {
    /// What was lexed.
    pub fn kind(&self) -> &TokenKind {
        &self.kind
    }

    /// Where, including any quotes or sigils.
    pub fn span(&self) -> Span {
        self.span
    }
}

/// Token kinds. Kinds without a payload take their text from the span.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TokenKind {
    /// An unquoted identifier or keyword: a letter, or a run of `_` and `@`
    /// followed by a letter or digit, then letters, digits and `_`.
    Word,
    /// A backtick-quoted identifier; the payload has doubled backticks
    /// collapsed.
    QuotedIdent(String),
    /// An unquoted name containing `*`, lexed only inside `KEEP` and `DROP`.
    NamePattern,
    /// An unquoted source pattern, lexed only where a command takes source
    /// patterns: the sources of `FROM` and `TS`, the policy of `ENRICH`, the
    /// index of `LOOKUP JOIN`, and the parameters of `PROMQL`.
    SourcePattern,
    /// Decimal digits.
    Integer,
    /// A number with a fraction, an exponent, or both.
    Decimal,
    /// A `"..."` or `"""..."""` string; the payload is the decoded value.
    String(String),
    /// A value parameter: `?`, `?1` or `?name`.
    Param(ParamName),
    /// An identifier parameter: `??`, `??1` or `??name`.
    IdentParam(ParamName),
    /// The parenthesised query of `PROMQL`, parentheses included.
    PromqlRegion,
    /// `|`
    Pipe,
    /// `;`, ending a `SET` directive.
    Semicolon,
    /// `,`
    Comma,
    /// `.`
    Dot,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `=`
    Assign,
    /// `==`
    Eq,
    /// `!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Star,
    /// `/`
    Slash,
    /// `%`
    Percent,
    /// `::`
    Cast,
    /// `:`
    Colon,
}

/// Lexes one ES|QL statement into tokens with their spans.
///
/// Refuses a statement longer than [`Limits::max_statement_bytes`] before
/// lexing, and one that would produce more than [`Limits::max_tokens`]
/// tokens at the first token past the limit. Nesting and stage limits are
/// checked by [`crate::parse`], not here.
pub fn lex(source: &str, limits: &Limits) -> Result<Vec<Token>, Error> {
    if source.len() > limits.max_statement_bytes {
        return Err(Error::LimitExceeded {
            limit: LimitKind::StatementBytes,
            max: limits.max_statement_bytes,
            span: Span::new(limits.max_statement_bytes, source.len()),
        });
    }
    let mut lexer = Lexer {
        src: source,
        bytes: source.as_bytes(),
        pos: 0,
        mode: Mode::CommandStart,
        restore: Vec::new(),
        after_in: false,
    };
    let mut tokens = Vec::new();
    loop {
        lexer.skip_trivia()?;
        if lexer.pos >= lexer.bytes.len() {
            return Ok(tokens);
        }
        let token = match lexer.mode {
            Mode::Promql if lexer.byte(0) == Some(b'(') => lexer.promql_region()?,
            Mode::SourceStart | Mode::Source | Mode::SingleSource | Mode::Promql => {
                lexer.source_token()?
            }
            Mode::Pattern => lexer.pattern_token()?,
            Mode::CommandStart | Mode::Expression | Mode::Lookup | Mode::Fork => {
                lexer.expression_token()?
            }
        };
        if tokens.len() >= limits.max_tokens {
            return Err(Error::LimitExceeded {
                limit: LimitKind::Tokens,
                max: limits.max_tokens,
                span: token.span,
            });
        }
        lexer.advance_mode(&token);
        tokens.push(token);
    }
}

/// The earlier name of [`lex`], kept for existing callers.
pub use self::lex as tokenize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    CommandStart,
    Expression,
    /// A source name is expected: after `FROM`, `TS` or a comma in their
    /// source list.
    SourceStart,
    Source,
    /// One source pattern: the policy after `ENRICH` or the index after
    /// `LOOKUP JOIN`.
    SingleSource,
    /// After `LOOKUP`, where `JOIN` is expected.
    Lookup,
    Pattern,
    /// Between the branches of a `FORK`.
    Fork,
    /// The parameters of `PROMQL`, before its query.
    Promql,
}

struct Lexer<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
    mode: Mode,
    /// For each open `(`, the mode to resume after its `)`.
    restore: Vec<Mode>,
    /// Whether the last token was the word `IN`.
    after_in: bool,
}

fn is_word_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'@'
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'@'
}

fn is_source_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'*' | b':')
}

/// The first byte at or after `pos` that is not whitespace or a comment, or
/// `Err` with the start of an unterminated block comment.
fn trivia_end(bytes: &[u8], mut pos: usize) -> Result<usize, usize> {
    loop {
        match (bytes.get(pos), bytes.get(pos + 1)) {
            (Some(b' ' | b'\t' | b'\r' | b'\n'), _) => pos += 1,
            (Some(b'/'), Some(b'/')) => {
                while bytes.get(pos).is_some_and(|&b| b != b'\n') {
                    pos += 1;
                }
            }
            (Some(b'/'), Some(b'*')) => {
                let start = pos;
                pos += 2;
                let mut depth = 1usize;
                while depth > 0 {
                    match (bytes.get(pos), bytes.get(pos + 1)) {
                        (None, _) => return Err(start),
                        (Some(b'/'), Some(b'*')) => {
                            depth += 1;
                            pos += 2;
                        }
                        (Some(b'*'), Some(b'/')) => {
                            depth -= 1;
                            pos += 2;
                        }
                        _ => pos += 1,
                    }
                }
            }
            _ => return Ok(pos),
        }
    }
}

impl<'a> Lexer<'a> {
    fn byte(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn text(&self, start: usize, end: usize) -> &'a str {
        self.src.get(start..end).unwrap_or("")
    }

    /// Whether the next token, after any trivia, is the word `keyword`.
    fn next_word_is(&self, keyword: &str) -> bool {
        let Ok(start) = trivia_end(self.bytes, self.pos) else {
            return false;
        };
        let end = start + keyword.len();
        self.bytes
            .get(start..end)
            .is_some_and(|w| w.eq_ignore_ascii_case(keyword.as_bytes()))
            && !self.bytes.get(end).copied().is_some_and(is_word_byte)
    }

    /// The mode the first word of a command selects.
    fn command_mode(word: &str) -> Mode {
        const MODES: &[(&str, Mode)] = &[
            ("FROM", Mode::SourceStart),
            ("TS", Mode::SourceStart),
            ("KEEP", Mode::Pattern),
            ("DROP", Mode::Pattern),
            ("ENRICH", Mode::SingleSource),
            ("LOOKUP", Mode::Lookup),
            ("FORK", Mode::Fork),
            ("PROMQL", Mode::Promql),
        ];
        MODES
            .iter()
            .find(|(keyword, _)| keyword.eq_ignore_ascii_case(word))
            .map_or(Mode::Expression, |&(_, mode)| mode)
    }

    fn advance_mode(&mut self, token: &Token) {
        let after_in = std::mem::replace(&mut self.after_in, false);
        self.mode = match (self.mode, &token.kind) {
            (_, TokenKind::Pipe | TokenKind::Semicolon) => Mode::CommandStart,
            (_, TokenKind::RParen) => self.restore.pop().unwrap_or(Mode::Expression),
            (Mode::Fork, TokenKind::LParen) => {
                self.restore.push(Mode::Fork);
                Mode::CommandStart
            }
            (Mode::SourceStart, TokenKind::LParen) => {
                self.restore.push(Mode::Source);
                Mode::CommandStart
            }
            (Mode::Expression, TokenKind::LParen) if after_in && self.next_word_is("FROM") => {
                self.restore.push(Mode::Expression);
                Mode::CommandStart
            }
            (mode, TokenKind::LParen) => {
                self.restore.push(match mode {
                    Mode::Pattern => Mode::Pattern,
                    _ => Mode::Expression,
                });
                Mode::Expression
            }
            (Mode::CommandStart, TokenKind::Word) => {
                Self::command_mode(self.text(token.span.start, token.span.end))
            }
            (Mode::CommandStart | Mode::SingleSource, _) => Mode::Expression,
            (Mode::Lookup, TokenKind::Word) => Mode::SingleSource,
            (Mode::Lookup, _) => Mode::Expression,
            (Mode::SourceStart, _) => Mode::Source,
            (Mode::Source, TokenKind::Comma) => Mode::SourceStart,
            // Only METADATA lexes as a word in source mode.
            (Mode::Source, TokenKind::Word) => Mode::Expression,
            (Mode::Promql, TokenKind::PromqlRegion) => Mode::Expression,
            (Mode::Expression, TokenKind::Word) => {
                self.after_in = self
                    .text(token.span.start, token.span.end)
                    .eq_ignore_ascii_case("IN");
                Mode::Expression
            }
            (mode, _) => mode,
        };
    }

    fn unexpected_char(&self, at: usize) -> Error {
        let found = self
            .src
            .get(at..)
            .and_then(|rest| rest.chars().next())
            .unwrap_or(char::REPLACEMENT_CHARACTER);
        Error::UnexpectedCharacter {
            found,
            span: Span::new(at, at + found.len_utf8()),
        }
    }

    fn skip_trivia(&mut self) -> Result<(), Error> {
        match trivia_end(self.bytes, self.pos) {
            Ok(end) => {
                self.pos = end;
                Ok(())
            }
            Err(start) => Err(Error::UnterminatedComment {
                span: Span::new(start, start + 2),
            }),
        }
    }

    fn punct(&mut self, kind: TokenKind, len: usize) -> Token {
        let start = self.pos;
        self.pos += len;
        Token {
            kind,
            span: Span::new(start, self.pos),
        }
    }

    fn expression_token(&mut self) -> Result<Token, Error> {
        let start = self.pos;
        let Some(b) = self.byte(0) else {
            return Err(self.unexpected_char(start));
        };
        let next = self.byte(1);
        Ok(match b {
            b'|' => self.punct(TokenKind::Pipe, 1),
            b';' => self.punct(TokenKind::Semicolon, 1),
            b',' => self.punct(TokenKind::Comma, 1),
            b'(' => self.punct(TokenKind::LParen, 1),
            b')' => self.punct(TokenKind::RParen, 1),
            b'[' => self.punct(TokenKind::LBracket, 1),
            b']' => self.punct(TokenKind::RBracket, 1),
            b'{' => self.punct(TokenKind::LBrace, 1),
            b'}' => self.punct(TokenKind::RBrace, 1),
            b'+' => self.punct(TokenKind::Plus, 1),
            b'-' => self.punct(TokenKind::Minus, 1),
            b'*' => self.punct(TokenKind::Star, 1),
            b'/' => self.punct(TokenKind::Slash, 1),
            b'%' => self.punct(TokenKind::Percent, 1),
            b'=' if next == Some(b'=') => self.punct(TokenKind::Eq, 2),
            b'=' => self.punct(TokenKind::Assign, 1),
            b'!' if next == Some(b'=') => self.punct(TokenKind::NotEq, 2),
            b'<' if next == Some(b'=') => self.punct(TokenKind::LtEq, 2),
            b'<' => self.punct(TokenKind::Lt, 1),
            b'>' if next == Some(b'=') => self.punct(TokenKind::GtEq, 2),
            b'>' => self.punct(TokenKind::Gt, 1),
            b':' if next == Some(b':') => self.punct(TokenKind::Cast, 2),
            b':' => self.punct(TokenKind::Colon, 1),
            b'.' if next.is_some_and(|n| n.is_ascii_digit()) => self.number(),
            b'.' => self.punct(TokenKind::Dot, 1),
            b'"' => self.string()?,
            b'`' => self.quoted_ident()?,
            b'?' => self.param(),
            b'0'..=b'9' => self.number(),
            b if is_word_start(b) => self.word(false)?,
            _ => return Err(self.unexpected_char(start)),
        })
    }

    fn source_token(&mut self) -> Result<Token, Error> {
        let start = self.pos;
        match self.byte(0) {
            Some(b',') => Ok(self.punct(TokenKind::Comma, 1)),
            Some(b'|') => Ok(self.punct(TokenKind::Pipe, 1)),
            Some(b'"') => self.string(),
            Some(b) if is_source_byte(b) => {
                while self.byte(0).is_some_and(is_source_byte) {
                    self.pos += 1;
                }
                let kind = if self.mode == Mode::Source
                    && self.text(start, self.pos).eq_ignore_ascii_case("METADATA")
                {
                    TokenKind::Word
                } else {
                    TokenKind::SourcePattern
                };
                Ok(Token {
                    kind,
                    span: Span::new(start, self.pos),
                })
            }
            _ => self.expression_token(),
        }
    }

    fn pattern_token(&mut self) -> Result<Token, Error> {
        match self.byte(0) {
            Some(b) if is_word_start(b) || b == b'*' => self.word(true),
            _ => self.expression_token(),
        }
    }

    /// The query of `PROMQL`: from `(` to the matching `)`. Parentheses
    /// inside PromQL strings (`"..."`, `'...'`, `` `...` ``) and `#` comments
    /// do not count.
    fn promql_region(&mut self) -> Result<Token, Error> {
        let start = self.pos;
        let unterminated = Error::UnterminatedPromql {
            span: Span::new(start, start + 1),
        };
        let mut depth = 0usize;
        let mut i = start;
        loop {
            match self.bytes.get(i) {
                None => return Err(unterminated),
                Some(b'(') => depth += 1,
                Some(b')') => {
                    depth -= 1;
                    if depth == 0 {
                        self.pos = i + 1;
                        return Ok(Token {
                            kind: TokenKind::PromqlRegion,
                            span: Span::new(start, self.pos),
                        });
                    }
                }
                Some(&quote @ (b'"' | b'\'' | b'`')) => {
                    i += 1;
                    loop {
                        match self.bytes.get(i) {
                            None => return Err(unterminated),
                            Some(b'\\') if quote != b'`' => i += 2,
                            Some(&b) if b == quote => break,
                            Some(_) => i += 1,
                        }
                    }
                }
                Some(b'#') => {
                    while self.bytes.get(i + 1).is_some_and(|&b| b != b'\n') {
                        i += 1;
                    }
                }
                Some(_) => {}
            }
            i += 1;
        }
    }

    /// A word, or with `allow_star` a name pattern when the run contains `*`.
    fn word(&mut self, allow_star: bool) -> Result<Token, Error> {
        let start = self.pos;
        while matches!(self.byte(0), Some(b'_' | b'@')) {
            self.pos += 1;
        }
        let lead_end = self.pos;
        let mut has_alnum = false;
        let mut has_star = false;
        while let Some(b) = self.byte(0) {
            if b.is_ascii_alphanumeric() {
                has_alnum = true;
            } else if allow_star && b == b'*' {
                has_star = true;
            } else if b != b'_' {
                break;
            }
            self.pos += 1;
        }
        let span = Span::new(start, self.pos);
        if has_star {
            return Ok(Token {
                kind: TokenKind::NamePattern,
                span,
            });
        }
        if lead_end > start && !has_alnum {
            return Err(self.unexpected_char(start));
        }
        Ok(Token {
            kind: TokenKind::Word,
            span,
        })
    }

    fn digits(&mut self) {
        while self.byte(0).is_some_and(|b| b.is_ascii_digit()) {
            self.pos += 1;
        }
    }

    fn number(&mut self) -> Token {
        let start = self.pos;
        self.digits();
        let mut decimal = false;
        if self.byte(0) == Some(b'.') {
            decimal = true;
            self.pos += 1;
            self.digits();
        }
        let exponent_digits_at = match (self.byte(0), self.byte(1), self.byte(2)) {
            (Some(b'e' | b'E'), Some(d), _) if d.is_ascii_digit() => Some(1),
            (Some(b'e' | b'E'), Some(b'+' | b'-'), Some(d)) if d.is_ascii_digit() => Some(2),
            _ => None,
        };
        if let Some(skip) = exponent_digits_at {
            decimal = true;
            self.pos += skip;
            self.digits();
        }
        Token {
            kind: if decimal {
                TokenKind::Decimal
            } else {
                TokenKind::Integer
            },
            span: Span::new(start, self.pos),
        }
    }

    fn string(&mut self) -> Result<Token, Error> {
        let start = self.pos;
        if self.bytes.get(start..start + 3) == Some(b"\"\"\"".as_slice()) {
            return self.triple_string(start);
        }
        let mut value = String::new();
        let mut i = start + 1;
        let mut run = i;
        loop {
            match self.bytes.get(i) {
                None | Some(b'\n' | b'\r') => {
                    return Err(Error::UnterminatedString {
                        span: Span::new(start, start + 1),
                    });
                }
                Some(b'"') => {
                    value.push_str(self.text(run, i));
                    self.pos = i + 1;
                    return Ok(Token {
                        kind: TokenKind::String(value),
                        span: Span::new(start, self.pos),
                    });
                }
                Some(b'\\') => {
                    value.push_str(self.text(run, i));
                    let decoded = match self.bytes.get(i + 1) {
                        Some(b'"') => '"',
                        Some(b'\\') => '\\',
                        Some(b'n') => '\n',
                        Some(b'r') => '\r',
                        Some(b't') => '\t',
                        None | Some(b'\n' | b'\r') => {
                            return Err(Error::UnterminatedString {
                                span: Span::new(start, start + 1),
                            });
                        }
                        Some(_) => {
                            let escaped = self
                                .src
                                .get(i + 1..)
                                .and_then(|rest| rest.chars().next())
                                .map_or(1, char::len_utf8);
                            return Err(Error::InvalidEscape {
                                span: Span::new(i, i + 1 + escaped),
                            });
                        }
                    };
                    value.push(decoded);
                    i += 2;
                    run = i;
                }
                Some(_) => i += 1,
            }
        }
    }

    /// `"""..."""`: no escapes, newlines allowed. The body ends at the first
    /// run of three or more quotes; quotes in that run beyond the last three
    /// belong to the body.
    fn triple_string(&mut self, start: usize) -> Result<Token, Error> {
        let body = start + 3;
        let Some(found) = self.text(body, self.bytes.len()).find("\"\"\"") else {
            return Err(Error::UnterminatedString {
                span: Span::new(start, body),
            });
        };
        let mut end = body + found + 3;
        while self.bytes.get(end) == Some(&b'"') {
            end += 1;
        }
        self.pos = end;
        Ok(Token {
            kind: TokenKind::String(self.text(body, end - 3).to_owned()),
            span: Span::new(start, end),
        })
    }

    fn quoted_ident(&mut self) -> Result<Token, Error> {
        let start = self.pos;
        let mut value = String::new();
        let mut i = start + 1;
        let mut run = i;
        loop {
            match self.bytes.get(i) {
                None => {
                    return Err(Error::UnterminatedQuotedIdentifier {
                        span: Span::new(start, start + 1),
                    });
                }
                Some(b'`') if self.bytes.get(i + 1) == Some(&b'`') => {
                    value.push_str(self.text(run, i));
                    value.push('`');
                    i += 2;
                    run = i;
                }
                Some(b'`') => {
                    value.push_str(self.text(run, i));
                    self.pos = i + 1;
                    return Ok(Token {
                        kind: TokenKind::QuotedIdent(value),
                        span: Span::new(start, self.pos),
                    });
                }
                Some(_) => i += 1,
            }
        }
    }

    fn param(&mut self) -> Token {
        let start = self.pos;
        self.pos += 1;
        let identifier = self.byte(0) == Some(b'?');
        if identifier {
            self.pos += 1;
        }
        let name_start = self.pos;
        let name = match self.byte(0) {
            Some(b) if b.is_ascii_digit() => {
                self.digits();
                ParamName::Numbered(self.text(name_start, self.pos).to_owned())
            }
            Some(b) if b.is_ascii_alphabetic() || b == b'_' => {
                while self
                    .byte(0)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    self.pos += 1;
                }
                ParamName::Named(self.text(name_start, self.pos).to_owned())
            }
            _ => ParamName::Positional,
        };
        Token {
            kind: if identifier {
                TokenKind::IdentParam(name)
            } else {
                TokenKind::Param(name)
            },
            span: Span::new(start, self.pos),
        }
    }
}
