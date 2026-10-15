//! Byte-offset spans into statement text, and their 1-based line and column.

/// A half-open range `[start, end)` of byte offsets into the statement text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
    /// Offset of the first byte.
    pub start: usize,
    /// Offset one past the last byte.
    pub end: usize,
}

impl Span {
    /// A span covering `[start, end)`.
    pub const fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// The span from the start of `self` to the end of `other`.
    pub const fn to(self, other: Span) -> Span {
        Span::new(self.start, other.end)
    }

    /// The text this span covers, or `None` when it does not fall on
    /// character boundaries of `source`.
    // guarded-esql-parse-allow: indexes the text a span came from; reads no syntax.
    pub fn slice<'a>(&self, source: &'a str) -> Option<&'a str> {
        source.get(self.start..self.end)
    }

    /// The 1-based line and column of the span's first byte.
    // guarded-esql-parse-allow: counts lines and characters; reads no syntax.
    pub fn line_col(&self, source: &str) -> LineCol {
        LineCol::at(source, self.start)
    }
}

/// A 1-based line and column. Lines are separated by `\n`; the column counts
/// Unicode scalar values, not bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LineCol {
    /// 1-based line number.
    pub line: usize,
    /// 1-based column, in characters.
    pub column: usize,
}

impl LineCol {
    /// The line and column of byte `offset` in `source`. An offset inside a
    /// multi-byte character reports that character's column; an offset at or
    /// past the end reports the position just after the last character.
    // guarded-esql-parse-allow: counts lines and characters; reads no syntax.
    pub fn at(source: &str, offset: usize) -> LineCol {
        let mut line = 1;
        let mut column = 1;
        for (index, c) in source.char_indices() {
            if index + c.len_utf8() > offset {
                break;
            }
            if c == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        LineCol { line, column }
    }
}
