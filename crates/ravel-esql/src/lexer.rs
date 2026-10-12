//! Command-aware tokenizer for ES|QL statement text.
//!
//! Text after `FROM` is a list of source patterns (`logs-prod-*`,
//! `cluster:index`, `"quoted"`), and the name lists of `KEEP` and `DROP` may
//! contain `*` wildcards; neither lexes like an expression. The lexer
//! therefore tracks which command it is inside: the first word after the
//! start of the statement or after a `|` selects the mode for the rest of
//! that command, and `METADATA` ends the source list of a `FROM`. A source
//! name is expected first and after each comma, so a source named `metadata`
//! in either place lexes as a source, not as that keyword.
//!
//! Whitespace, `//` line comments and `/* */` block comments (which nest)
//! separate tokens and produce none. Keywords are not distinguished here: a
//! keyword is a [`TokenKind::Word`] whose text the parser compares without
//! regard to case, and the token's span keeps the original spelling.

use crate::ast::ParamName;
use crate::error::Error;
use crate::limits::{LimitKind, Limits};
use crate::span::Span;

/// One token and the bytes it was lexed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// What was lexed.
    pub kind: TokenKind,
    /// Where, including any quotes or sigils.
    pub span: Span,
}

/// Token kinds. Kinds without a payload take their text from the span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenKind {
    /// An unquoted identifier or keyword: a letter, or a run of `_` and `@`
    /// followed by a letter or digit, then letters, digits and `_`.
    Word,
    /// A backtick-quoted identifier; the payload has doubled backticks
    /// collapsed.
    QuotedIdent(String),
    /// An unquoted name containing `*`, lexed only inside `KEEP` and `DROP`.
    NamePattern,
    /// An unquoted source pattern, lexed only inside the source list of
    /// `FROM`.
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
    /// `|`
    Pipe,
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

/// Lexes `source`, refusing it once it would produce more than
/// [`Limits::max_tokens`] tokens. Other limits are not checked here.
pub fn tokenize(source: &str, limits: &Limits) -> Result<Vec<Token>, Error> {
    let mut lexer = Lexer {
        src: source,
        bytes: source.as_bytes(),
        pos: 0,
        mode: Mode::CommandStart,
    };
    let mut tokens = Vec::new();
    loop {
        lexer.skip_trivia()?;
        if lexer.pos >= lexer.bytes.len() {
            return Ok(tokens);
        }
        let token = match lexer.mode {
            Mode::SourceStart | Mode::Source => lexer.source_token()?,
            Mode::Pattern => lexer.pattern_token()?,
            Mode::CommandStart | Mode::Expression => lexer.expression_token()?,
        };
        if tokens.len() >= limits.max_tokens {
            return Err(Error::LimitExceeded {
                limit: LimitKind::Tokens,
                max: limits.max_tokens,
                span: token.span,
            });
        }
        lexer.mode = lexer.next_mode(&token);
        tokens.push(token);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    CommandStart,
    Expression,
    /// A source name is expected: after `FROM` or a comma in its source list.
    SourceStart,
    Source,
    Pattern,
}

struct Lexer<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
    mode: Mode,
}

fn is_word_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b'@'
}

fn is_source_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'*' | b':')
}

impl<'a> Lexer<'a> {
    fn byte(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn text(&self, start: usize, end: usize) -> &'a str {
        self.src.get(start..end).unwrap_or("")
    }

    fn next_mode(&self, token: &Token) -> Mode {
        match (self.mode, &token.kind) {
            (_, TokenKind::Pipe) => Mode::CommandStart,
            (Mode::CommandStart, TokenKind::Word) => {
                let word = self.text(token.span.start, token.span.end);
                if word.eq_ignore_ascii_case("FROM") {
                    Mode::SourceStart
                } else if word.eq_ignore_ascii_case("KEEP") || word.eq_ignore_ascii_case("DROP") {
                    Mode::Pattern
                } else {
                    Mode::Expression
                }
            }
            (Mode::CommandStart, _) => Mode::Expression,
            (Mode::SourceStart, _) => Mode::Source,
            (Mode::Source, TokenKind::Comma) => Mode::SourceStart,
            // Only METADATA lexes as a word in source mode.
            (Mode::Source, TokenKind::Word) => Mode::Expression,
            (mode, _) => mode,
        }
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
        loop {
            match (self.byte(0), self.byte(1)) {
                (Some(b' ' | b'\t' | b'\r' | b'\n'), _) => self.pos += 1,
                (Some(b'/'), Some(b'/')) => {
                    while let Some(b) = self.byte(0) {
                        if b == b'\n' {
                            break;
                        }
                        self.pos += 1;
                    }
                }
                (Some(b'/'), Some(b'*')) => self.block_comment()?,
                _ => return Ok(()),
            }
        }
    }

    fn block_comment(&mut self) -> Result<(), Error> {
        let start = self.pos;
        self.pos += 2;
        let mut depth = 1usize;
        while depth > 0 {
            match (self.byte(0), self.byte(1)) {
                (None, _) => {
                    return Err(Error::UnterminatedComment {
                        span: Span::new(start, start + 2),
                    });
                }
                (Some(b'/'), Some(b'*')) => {
                    depth += 1;
                    self.pos += 2;
                }
                (Some(b'*'), Some(b'/')) => {
                    depth -= 1;
                    self.pos += 2;
                }
                _ => self.pos += 1,
            }
        }
        Ok(())
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
