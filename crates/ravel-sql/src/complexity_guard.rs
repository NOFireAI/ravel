//! Pre-parse structural-complexity guard for raw SQL statement text (issue
//! #1680).
//!
//! `DFParser::parse_sql` builds a `sqlparser` AST, and every walk over that
//! AST in this crate (`crate::validate`'s write and excluded-function
//! visitors, `crate::page_plan`'s rewrites, DataFusion's own
//! SQL-to-`LogicalPlan` conversion) recurses once per tree level, as does the
//! tree's `Drop`. The parser's own recursion limit does not bound tree depth:
//! it consumes a run of same-precedence infix operators in a *loop*, so
//! `SELECT 1+1+1+...` parses at a nesting depth of one while building a tree
//! one level deep per operator. The first walk then uses one stack frame per
//! operator on the 2 MiB stack a `tokio::main` worker thread gets, and a Rust
//! stack overflow is an abort, not a catchable panic: the whole process dies,
//! taking every other tenant's queries (and, in `--mode all`, the in-flight
//! ingest buffers) with it. This module scans the raw statement text, so the
//! reject happens before `DFParser::parse_sql` is ever called.
//!
//! The bound enforced is deliberately not "nesting depth". The same design
//! was tried in the PromQL guard this module follows
//! (`ravel_promql::complexity_guard`, issue #529) and proven unsound by
//! experiment: a flat operator chain nests nowhere and still builds an
//! arbitrarily deep tree. Rather than enumerate every construct that can
//! deepen a tree, this guard bounds a simpler, sound invariant: a
//! recursive-descent parser cannot produce more tree levels than it has
//! structural characters to consume, so capping
//! `count(non-whitespace characters outside literals and comments)` caps
//! parse depth, AST depth, and therefore every downstream walk depth by
//! construction, for every construct, tested or not.
//!
//! # What is excluded, and why the exclusions are sound
//!
//! Literal payloads are excluded because they are single tokens to the
//! parser: a `LIKE` pattern, an `IN` list of long strings, or an embedded
//! JSON document can be long without adding one tree level. Comments are
//! excluded because they are not tokens at all.
//!
//! The exclusion regions must be a *subset* of the regions `sqlparser`'s
//! tokenizer itself treats as opaque; a region this scan skips that the
//! tokenizer does not would hide real structure and make the guard unsound.
//! So the scan mirrors the tokenizer's rules for the `GenericDialect`
//! DataFusion parses with ([`DFParserBuilder`'s default dialect](https://docs.rs/datafusion-sql)):
//!
//! - `'...'` string literals, closed by the next `'` (doubling a quote to
//!   escape it reads as close-then-reopen, which excludes exactly the same
//!   text). `GenericDialect::supports_string_literal_backslash_escape()` is
//!   false, so a backslash escapes nothing here either.
//! - `"..."` and `` `...` `` delimited identifiers, which
//!   `GenericDialect::is_delimited_identifier_start` accepts and which are
//!   one token each.
//! - `$tag$...$tag$` and `$$...$$` dollar-quoted strings. These are tokenized
//!   for every dialect that is not a dollar-placeholder dialect, and
//!   `GenericDialect` is not one. Skipping them is not an optimization: a
//!   scan that did not know about them would read the `'` inside
//!   `$$ ' $$ 1+1+1...` as opening a string and skip the operator chain that
//!   follows, which is exactly the unsound direction.
//! - `--` line comments, and `/* ... */` block comments, nested, because
//!   `GenericDialect::supports_nested_comments()` is true.
//!
//! Each literal's opening delimiter is itself counted, so every literal token
//! costs at least one structural character no matter how long its payload is.
//!
//! # Calibrating [`MAX_STATEMENT_COMPLEXITY`]
//!
//! Measured on a 2 MiB stack (the tokio worker default the server runs on),
//! release profile, by spawning
//! `std::thread::Builder::new().stack_size(2 << 20)` and growing each
//! construct until the process aborted. Counts below are structural
//! characters as this module counts them, not raw bytes:
//!
//! | construct                             | chars/level | `validate` survives | `validate` aborts | planner survives | planner aborts |
//! |---------------------------------------|:-----------:|:-------------------:|:-----------------:|:----------------:|:--------------:|
//! | `SELECT 1+1+1+...` (binary chain)     | 2           | 50,007              | 60,007            | 1,807            | 1,907          |
//! | `SELECT 'a'\|\|'a'\|\|...` (concat)   | 3           | 75,007              | 90,007            | 2,707            | 3,307          |
//! | `... WHERE 1=1 AND 1=1 AND ...`       | 6           | 120,026             | 240,026           | 6,626            | above 6,626    |
//! | `SELECT ((((1))))` (paren nesting)    | 2           | rejected by the parser recursion limit at depth 50 | | | |
//! | `SELECT * FROM (SELECT * FROM (...))` | 13          | rejected by the parser recursion limit at depth 50 | | | |
//!
//! The binding floor is not this crate's own walk. `validate`'s visitors
//! survive 50,007 structural characters, but a statement they accept is then
//! handed to DataFusion's SQL-to-`LogicalPlan` conversion, which walks the
//! same tree with much larger frames and aborts at 1,907 characters of the
//! same chain. So the guard is calibrated against that consumer, not against
//! its own caller: [`MAX_STATEMENT_COMPLEXITY`] is set to a third of the
//! tightest measured survival figure. The remaining margin absorbs two things
//! this guard must not have to prove separately: that the worker stack is
//! empty when the statement runs (it is not; the axum/tower/hyper or tonic
//! frames below it are already tens of kilobytes, and the probe's thread was
//! nearly empty), and that a future compiler or dependency version keeps the
//! same frame sizes.
//!
//! A debug build's frames are roughly two orders of magnitude larger than a
//! release build's: the same binary chain aborts inside `validate` alone at
//! 407 structural characters unoptimized. The published server image is a
//! release build (`Dockerfile`), and a bound low enough to protect a debug
//! build would reject ordinary analytic SQL, so the bound is sized for the
//! release profile.
//!
//! A long flat `IN` list is not itself a depth risk (`sqlparser` holds it as
//! a `Vec`; 500,000 elements parse and walk without incident, confirmed by
//! the same probe), but this guard does not special-case it: the whole point
//! of the sound-invariant approach is to not depend on a per-construct safety
//! argument. A list of quoted values stays cheap anyway, since each element
//! costs one character for its opening quote plus one for the comma; a list
//! of long numeric literals is the shape that spends the budget fastest.

use std::fmt;

/// Maximum count of non-whitespace characters outside literals and comments a
/// SQL statement's text may contain. See the module documentation for how
/// this was measured and why it bounds parse depth, AST depth, and every
/// AST-walk depth, for every construct rather than just the ones tested.
pub const MAX_STATEMENT_COMPLEXITY: usize = 600;

/// A statement's structural character count exceeded
/// [`MAX_STATEMENT_COMPLEXITY`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementTooComplex {
    /// The count at which the scan stopped (one past the max).
    pub count: usize,
    /// [`MAX_STATEMENT_COMPLEXITY`], carried alongside the measured count so
    /// callers can report both without reaching back into this module.
    pub max: usize,
}

impl fmt::Display for StatementTooComplex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "statement complexity {} exceeds the maximum of {} structural characters; simplify the statement",
            self.count, self.max
        )
    }
}

impl std::error::Error for StatementTooComplex {}

/// How the scan is currently reading the text.
enum Mode {
    /// Outside every literal and comment: characters here are counted.
    Normal,
    /// Inside a `'...'` string literal.
    Quoted(char),
    /// Inside a `$tag$...$tag$` dollar-quoted string, holding the full
    /// closing delimiter (`$tag$`, or `$$` when the tag is empty).
    Dollar(String),
    /// Inside a `--` line comment.
    LineComment,
    /// Inside a `/* ... */` block comment, holding the nesting depth.
    BlockComment(usize),
}

/// Scans `sql`'s raw text for excessive structural complexity, outside
/// literals and comments. Call this before `DFParser::parse_sql`, not after:
/// the abort this guards against happens while the parsed tree is walked, and
/// on a deep enough tree during the parse itself.
pub fn check(sql: &str) -> Result<(), StatementTooComplex> {
    let mut mode = Mode::Normal;
    let mut count: usize = 0;
    // Characters already consumed by a lookahead below, skipped when the
    // iterator reaches them.
    let mut skip: usize = 0;

    for (at, c) in sql.char_indices() {
        if skip > 0 {
            skip -= 1;
            continue;
        }
        let rest = &sql[at..];

        match mode {
            Mode::LineComment => {
                if c == '\n' {
                    mode = Mode::Normal;
                }
            }
            Mode::BlockComment(depth) => {
                if rest.starts_with("/*") {
                    mode = Mode::BlockComment(depth + 1);
                    skip = 1;
                } else if rest.starts_with("*/") {
                    mode = if depth == 1 {
                        Mode::Normal
                    } else {
                        Mode::BlockComment(depth - 1)
                    };
                    skip = 1;
                }
            }
            Mode::Quoted(delimiter) => {
                if c == delimiter {
                    mode = Mode::Normal;
                }
            }
            Mode::Dollar(ref end) => {
                if rest.starts_with(end.as_str()) {
                    skip = end.chars().count() - 1;
                    mode = Mode::Normal;
                }
            }
            Mode::Normal => {
                if rest.starts_with("--") {
                    mode = Mode::LineComment;
                    skip = 1;
                    continue;
                }
                if rest.starts_with("/*") {
                    mode = Mode::BlockComment(1);
                    skip = 1;
                    continue;
                }
                if c.is_whitespace() {
                    continue;
                }

                // Every literal token costs one structural character, however
                // long its payload is.
                count += 1;
                if count > MAX_STATEMENT_COMPLEXITY {
                    return Err(StatementTooComplex {
                        count,
                        max: MAX_STATEMENT_COMPLEXITY,
                    });
                }

                if c == '\'' || c == '"' || c == '`' {
                    mode = Mode::Quoted(c);
                    continue;
                }
                if c == '$'
                    && let Some(end) = dollar_delimiter(rest)
                {
                    skip = end.chars().count() - 1;
                    mode = Mode::Dollar(end);
                }
            }
        }
    }

    Ok(())
}

/// The closing delimiter of the dollar-quoted string opening at the start of
/// `rest` (whose first character must be `$`), or `None` when that `$` opens
/// no such string.
///
/// Mirrors the tokenizer: the tag runs while alphanumeric or `_`, and a `$`
/// must close it. `$1` and a bare `$` are placeholders, not literals, and
/// return `None` so their characters keep being counted.
fn dollar_delimiter(rest: &str) -> Option<String> {
    let tag_len = rest[1..]
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(rest.len() - 1);
    if rest[1 + tag_len..].starts_with('$') {
        Some(rest[..=1 + tag_len].to_string())
    } else {
        None
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn an_ordinary_statement_passes() {
        check("SELECT ts, value FROM samples WHERE ts > 0 ORDER BY ts LIMIT 10").expect("ordinary");
    }

    /// The abort payload from issue #1680: `SELECT 1` followed by `+1`
    /// 500,000 times. The scan stops one character past the bound rather than
    /// reading the whole 1 MiB body.
    #[test]
    fn the_flat_operator_chain_is_rejected_at_the_bound() {
        let sql = format!("SELECT 1{}", "+1".repeat(500_000));
        let err = check(&sql).expect_err("must be rejected");
        assert_eq!(
            err,
            StatementTooComplex {
                count: MAX_STATEMENT_COMPLEXITY + 1,
                max: MAX_STATEMENT_COMPLEXITY,
            }
        );
    }

    /// A string literal's payload is not structure: an operator chain written
    /// inside a literal adds one character (the opening quote), not one per
    /// operator.
    #[test]
    fn operator_characters_inside_a_string_literal_are_not_counted() {
        let payload = "+1".repeat(500_000);
        let sql = format!("SELECT body FROM logs WHERE body = '{payload}'");
        check(&sql).expect("a long literal is not structure");
    }

    /// `MAX_STATEMENT_COMPLEXITY - n` counted characters, as a run of `a`.
    fn filler(n: usize) -> String {
        "a".repeat(MAX_STATEMENT_COMPLEXITY - n)
    }

    /// The exact contribution of a string literal is one character, whatever
    /// its payload length: a literal plus `MAX - 1` other characters sits
    /// exactly at the bound, and one more character is over it.
    #[test]
    fn a_literal_costs_exactly_one_structural_character() {
        let payload = "z".repeat(10_000);
        assert_eq!(check(&format!("'{payload}'{}", filler(1))), Ok(()));
        assert_eq!(
            check(&format!("'{payload}'{}a", filler(1))),
            Err(StatementTooComplex {
                count: MAX_STATEMENT_COMPLEXITY + 1,
                max: MAX_STATEMENT_COMPLEXITY,
            })
        );
    }

    #[test]
    fn a_doubled_quote_does_not_end_the_literal() {
        // `'a''+1+1...'` is one literal holding a quote, so the operator run
        // inside it is still payload.
        let payload = "+1".repeat(500_000);
        let sql = format!("SELECT body FROM logs WHERE body = 'a''{payload}'");
        check(&sql).expect("a doubled quote stays inside the literal");
    }

    /// A quoted identifier is one token; its contents are not structure, and
    /// it costs exactly one character in either spelling.
    #[test]
    fn a_quoted_identifier_costs_exactly_one_structural_character() {
        let padding = "+".repeat(500_000);
        check(&format!("SELECT \"{padding}\" FROM samples")).expect("double-quoted identifier");
        check(&format!("SELECT `{padding}` FROM samples")).expect("backtick identifier");

        for sql in [
            format!("\"{padding}\"{}", filler(1)),
            format!("`{padding}`{}", filler(1)),
        ] {
            assert_eq!(check(&sql), Ok(()));
            assert_eq!(
                check(&format!("{sql}a")),
                Err(StatementTooComplex {
                    count: MAX_STATEMENT_COMPLEXITY + 1,
                    max: MAX_STATEMENT_COMPLEXITY,
                })
            );
        }
    }

    /// A dollar-quoted string is opaque to the parser, so it must be opaque
    /// here too. If it were not, the `'` inside it would be read as opening a
    /// string literal and the operator chain after it would be skipped: the
    /// unsound direction.
    #[test]
    fn a_dollar_quoted_string_cannot_hide_an_operator_chain() {
        let chain = "+1".repeat(500_000);
        let sql = format!("SELECT $$ ' $${chain}");
        let err = check(&sql).expect_err("the chain after the dollar string is structure");
        assert_eq!(err.count, MAX_STATEMENT_COMPLEXITY + 1);

        // Tagged form, same argument.
        let sql = format!("SELECT $tag$ ' $tag${chain}");
        let err = check(&sql).expect_err("the chain after the dollar string is structure");
        assert_eq!(err.count, MAX_STATEMENT_COMPLEXITY + 1);
    }

    /// A `$` that opens no dollar-quoted string is an ordinary character and
    /// keeps being counted.
    #[test]
    fn a_dollar_placeholder_is_counted_not_skipped() {
        let sql = format!("SELECT $1{}", "+1".repeat(MAX_STATEMENT_COMPLEXITY));
        let err = check(&sql).expect_err("must be rejected");
        assert_eq!(err.count, MAX_STATEMENT_COMPLEXITY + 1);
    }

    #[test]
    fn line_and_block_comments_are_not_counted() {
        let chain = "+1".repeat(500_000);
        check(&format!("SELECT 1 -- {chain}\n")).expect("line comment");
        check(&format!("SELECT 1 /* {chain} */")).expect("block comment");
        // Nested block comments, which the generic dialect supports.
        check(&format!("SELECT 1 /* /* {chain} */ */")).expect("nested block comment");
    }

    /// A comment costs exactly zero characters: a statement at the bound is
    /// still at the bound with a comment in front of it, in either spelling.
    #[test]
    fn a_comment_costs_exactly_zero_structural_characters() {
        for comment in ["-- z\n", "/* z */", "/* /* z */ */"] {
            let sql = format!("{comment}{}", filler(0));
            assert_eq!(check(&sql), Ok(()), "comment {comment}");
            assert_eq!(
                check(&format!("{sql}a")),
                Err(StatementTooComplex {
                    count: MAX_STATEMENT_COMPLEXITY + 1,
                    max: MAX_STATEMENT_COMPLEXITY,
                }),
                "comment {comment}"
            );
        }
    }

    /// A dollar-quoted string costs exactly one character, like the other
    /// literal forms.
    #[test]
    fn a_dollar_quoted_string_costs_exactly_one_structural_character() {
        let payload = "z".repeat(10_000);
        let sql = format!("$tag${payload}$tag${}", filler(1));
        assert_eq!(check(&sql), Ok(()));
        assert_eq!(
            check(&format!("{sql}a")),
            Err(StatementTooComplex {
                count: MAX_STATEMENT_COMPLEXITY + 1,
                max: MAX_STATEMENT_COMPLEXITY,
            })
        );
    }

    /// A comment marker inside a literal does not start a comment, so the
    /// structure after the literal is still counted.
    #[test]
    fn a_comment_marker_inside_a_literal_hides_nothing() {
        let sql = format!("SELECT '--'{}", "+1".repeat(MAX_STATEMENT_COMPLEXITY));
        let err = check(&sql).expect_err("must be rejected");
        assert_eq!(err.count, MAX_STATEMENT_COMPLEXITY + 1);
    }

    /// Exactly at the bound passes, one past it fails: the boundary is
    /// asserted on both sides rather than "somewhere around" it.
    #[test]
    fn the_bound_is_inclusive() {
        let at = "a".repeat(MAX_STATEMENT_COMPLEXITY);
        assert_eq!(check(&at), Ok(()));
        let over = "a".repeat(MAX_STATEMENT_COMPLEXITY + 1);
        assert_eq!(
            check(&over),
            Err(StatementTooComplex {
                count: MAX_STATEMENT_COMPLEXITY + 1,
                max: MAX_STATEMENT_COMPLEXITY,
            })
        );
    }

    /// Whitespace is free, so a statement formatted across many lines is not
    /// penalised for its formatting: the same seven structural characters
    /// pass however much whitespace surrounds them, and adding whitespace to
    /// a statement already at the bound does not push it over.
    #[test]
    fn whitespace_is_not_counted() {
        check(&format!("SELECT{}1", " ".repeat(100_000))).expect("whitespace is free");
        let at_bound: String = std::iter::repeat_n("a\n", MAX_STATEMENT_COMPLEXITY).collect();
        assert_eq!(check(&at_bound), Ok(()));
    }

    /// The message names complexity, and does not claim nesting or depth: the
    /// payload that triggers it has no nesting at all.
    #[test]
    fn the_message_names_complexity_not_nesting() {
        let err = check(&format!("SELECT 1{}", "+1".repeat(500_000))).expect_err("rejected");
        let msg = err.to_string();
        assert!(msg.contains("complexity"), "{msg}");
        assert!(msg.contains(&MAX_STATEMENT_COMPLEXITY.to_string()), "{msg}");
        assert!(!msg.contains("nest"), "{msg}");
        assert!(!msg.contains("depth"), "{msg}");
    }
}
