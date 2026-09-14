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
//! TOKENS to consume, so capping the count of tokens outside literals and
//! comments caps parse depth, AST depth, and therefore every downstream walk
//! depth by construction, for every construct, tested or not.
//!
//! The unit is the token, not the character. An earlier form of this guard
//! counted characters, which made the bound depend on how an author quoted an
//! identifier (`"ResolutionWidth"` cost 1 and `ResolutionWidth` cost 15) and
//! refused a dashboard's hundred-element numeric `IN` list at 1,036 while
//! admitting a four-hundred-element quoted one at 936, though the numeric one
//! is the shallower tree of the two. Counting a run of identifier, keyword, or
//! number characters as the one token it is removes that variance and leaves
//! the depth argument untouched, because a loop-consumed chain still spends
//! one operand token plus one operator token per level.
//!
//! # What is excluded, and why the exclusions are sound
//!
//! Literal payloads are excluded because they are single tokens to the
//! parser: a `LIKE` pattern, an `IN` list of long strings, or an embedded
//! JSON document can be long without adding one tree level. Ordinary comments
//! are excluded because they are not tokens at all. A `/*!...*/` hint comment
//! is NOT excluded, because for this dialect it is tokens: see below.
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
//! One exclusion that is UNSOUND, and is therefore not made: `/*!...*/`. `GenericDialect::supports_multiline_comment_hints()` is true
//! (sqlparser `dialect/generic.rs`), and the tokenizer re-tokenizes a block
//! comment whose body opens with `!` into real tokens (`tokenizer.rs`), on the
//! path `DFParserBuilder::build` uses. Skipping it would hide structure the
//! parser reads: measured before the fix, `SELECT 1/*!` + `+1` x2000 + `*/`
//! scored 7 while the tokenizer produced 4003 tokens from it. The scan
//! therefore counts a hint body rather than skipping it.
//!
//! The hint region gets its own scan mode, and that mode enters no sub-mode.
//! An earlier fix fell through to the ordinary counting mode instead, which
//! reopened the same bypass through another door: a `--` inside the hint body
//! put the scan in line-comment mode, which runs to newline or EOF, so
//! `SELECT 1/*! -- */` followed by a 4000-operator chain scored 5 while the
//! tokenizer emitted 8005 tokens. A quote, a backtick, a dollar quote and a
//! nested `/*` each open the same door. The tokenizer confines all of them to
//! the hint body and resumes at the matching `*/`, and a flat count of the
//! region does too.
//!
//! Both ways of mis-locating that matching `*/` over-count, so the mode is
//! sound whatever nesting does. Stopping early resumes ordinary counting
//! sooner and counts more text as top-level structure. Running past it counts
//! every remaining character. Neither direction can admit a statement the
//! tokenizer sees as deeper than the bound allows.
//!
//! Every token costs one unit, whatever its length: a literal through its
//! opening delimiter, an identifier or number through its first character.
//!
//! "Token" means what the tokenizer yields, not what looks like one word.
//! `GenericDialect::supports_numeric_prefix()` is false, so `1AND` is
//! `Number("1")` then `Word("AND")`, and the scan stops a digit run at the
//! first non-digit to match. Consuming it as one alphanumeric run charged one
//! unit for two tokens, and because each `AND` adds a tree level that made a
//! boolean chain cost one unit per level against the two every other construct
//! costs: `SELECT 1` + `AND 1` x998 scored exactly 1,000 units and built a
//! 998-level spine. A run that starts with a letter or `_` still consumes
//! alphanumerics, because `a1` really is one identifier, which is what keeps
//! the bound independent of quoting.
//!
//! # Calibrating [`MAX_STATEMENT_COMPLEXITY`]
//!
//! Measured on a 2 MiB stack (the tokio worker default the server runs on),
//! release profile, by spawning
//! `std::thread::Builder::new().stack_size(2 << 20)` and growing each
//! construct until the process aborted. Counts below are units as this module
//! counts them, not raw bytes. They were measured under the earlier
//! character-based rule, and every construct in the table is built from
//! single-character operands and operators, so each figure is the same under
//! the token rule; a table entry using multi-character operands would count
//! lower today:
//!
//! | construct                             | units/level | `validate` survives | `validate` aborts | planner survives | planner aborts |
//! |---------------------------------------|:-----------:|:-------------------:|:-----------------:|:----------------:|:--------------:|
//! | `SELECT 1+1+1+...` (binary chain)     | 2           | 50,007              | 60,007            | 1,807            | 1,907          |
//! | `SELECT 'a'\|\|'a'\|\|...` (concat)   | 3           | 75,007              | 90,007            | 2,707            | 3,307          |
//! | `... WHERE 1=1 AND 1=1 AND ...`       | 6           | 120,026             | 240,026           | 6,626            | above 6,626    |
//! | `SELECT ((((1))))` (paren nesting)    | 2           | rejected by the parser recursion limit at depth 50 | | | |
//! | `SELECT * FROM (SELECT * FROM (...))` | 13          | rejected by the parser recursion limit at depth 50 | | | |
//!
//! The binding floor is not this crate's own walk. `validate`'s visitors
//! survive 50,007 units, but a statement they accept is then
//! handed to DataFusion's SQL-to-`LogicalPlan` conversion, which walks the
//! same tree with much larger frames and aborts at 1,907 units of the
//! same chain, about 950 tree levels. So the guard is calibrated against that
//! consumer, not against its own caller.
//!
//! The margin is stated in levels rather than units, because that is
//! what the stack spends: no construct costs fewer than two units per
//! tree level (a claim that has been falsified once, by the numeric-prefix
//! case above, and is load-bearing enough to be worth re-checking against the
//! tokenizer whenever the run rule changes) (a binary operator and its right operand; parenthesis nesting
//! also costs two and is capped by the parser's recursion limit long before
//! this one bites), so a statement at [`MAX_STATEMENT_COMPLEXITY`] cannot
//! reach more than 500 levels against the ~950 measured to abort. Worst case,
//! an admitted statement can use a little over half the 2 MiB budget, leaving
//! the rest for the frames below it (the axum/tower/hyper or tonic stack, on
//! which the probe's nearly-empty thread says nothing) and for whatever frame
//! growth a future compiler or dependency version brings.
//!
//! The bound is not set lower than that because a lower one refuses real
//! analytic SQL. The largest statement in this repository's ClickBench corpus
//! (`benchmarks/clickbench/hits.corpus.json`, `q30_resolution_running_sums`,
//! 90 `SUM("ResolutionWidth" + n)` terms) counts 901 units under the earlier
//! character rule and fewer under the token rule,
//! and it is a flat projection list, not a deep tree.
//! `tests/statement_complexity.rs` pins that whole corpus as accepted, so a
//! later tightening of this bound fails there rather than in a user's query.
//!
//! A debug build's frames are roughly two orders of magnitude larger than a
//! release build's: the same binary chain aborts inside `validate` alone at
//! 407 units unoptimized. The published server image is a
//! release build (`Dockerfile`), and a bound low enough to protect a debug
//! build would reject ordinary analytic SQL, so the bound is sized for the
//! release profile.
//!
//! A long flat `IN` list is not itself a depth risk (`sqlparser` holds it as
//! a `Vec`; 500,000 elements parse and walk without incident, confirmed by
//! the same probe), but this guard does not special-case it: the whole point
//! of the sound-invariant approach is to not depend on a per-construct safety
//! argument. Every element costs the same two units, one for the value token
//! and one for the comma, whether the value is quoted or bare, so the list
//! that fits the bound is the same list either way.

use std::fmt;

/// Maximum count of tokens outside literals and comments a
/// SQL statement's text may contain. See the module documentation for how
/// this was measured and why it bounds parse depth, AST depth, and every
/// AST-walk depth, for every construct rather than just the ones tested.
pub const MAX_STATEMENT_COMPLEXITY: usize = 1_000;

/// A statement's token count outside literals and comments exceeded
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
            "statement complexity {} exceeds the maximum of {} tokens; simplify the statement",
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
    /// Inside a `/*!...*/` hint comment, holding the nesting depth. Every
    /// non-whitespace character here is counted and NO sub-mode is entered:
    /// see the `scan` comment where this mode is set.
    Hint(usize),
}

/// Scans `sql`'s raw text for excessive structural complexity, outside
/// literals and comments. Call this before `DFParser::parse_sql`, not after:
/// the abort this guards against happens while the parsed tree is walked, and
/// on a deep enough tree during the parse itself.
pub fn check(sql: &str) -> Result<(), StatementTooComplex> {
    let count = scan(sql, MAX_STATEMENT_COMPLEXITY);
    if count > MAX_STATEMENT_COMPLEXITY {
        return Err(StatementTooComplex {
            count,
            max: MAX_STATEMENT_COMPLEXITY,
        });
    }
    Ok(())
}

/// `sql`'s token count outside literals and comments, by the same rules
/// [`check`] applies.
///
/// [`check`] stops as soon as it knows the answer, so it reports a count only
/// on rejection; this scans the whole text. It exists for callers that need
/// the figure for an accepted statement (a corpus test asserting how much of
/// the budget real statements spend, an operator sizing the bound), not for
/// the request path.
pub fn structural_count(sql: &str) -> usize {
    scan(sql, usize::MAX)
}

/// Count tokens outside literals and comments, stopping as soon as the count exceeds
/// `stop_above`. The returned count is then `stop_above + 1`, never the full
/// figure: that is all [`check`] needs, and it keeps a 1 MiB adversarial body
/// from being scanned to its end.
fn scan(sql: &str, stop_above: usize) -> usize {
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
            Mode::Hint(depth) => {
                if rest.starts_with("/*") {
                    count += 2;
                    skip = 1;
                    mode = Mode::Hint(depth + 1);
                } else if rest.starts_with("*/") {
                    count += 2;
                    skip = 1;
                    mode = if depth == 1 {
                        Mode::Normal
                    } else {
                        Mode::Hint(depth - 1)
                    };
                } else if !c.is_whitespace() {
                    count += 1;
                }
                if count > stop_above {
                    return count;
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
                // `/*!...*/` is NOT opaque to this dialect. `GenericDialect`
                // returns true from `supports_multiline_comment_hints`, and the
                // tokenizer re-tokenizes a block comment whose body opens with
                // `!` into real tokens. Treating it as a comment skips a region
                // the parser reads, which is the unsound direction: a measured
                // probe scored `SELECT 1/*!` + `+1` x2000 + `*/` at 7 while the
                // tokenizer produced 4003 tokens from it.
                //
                // The region gets its OWN mode rather than falling through to
                // `Normal`, and that mode enters no sub-mode. Falling through
                // was tried and reopened the same bypass through another door:
                // a `--` inside the hint body put the scan in `LineComment`,
                // which runs to newline or EOF, so `SELECT 1/*! -- */` + a
                // 4000-operator chain scored 5 while the tokenizer emitted
                // 8005 tokens. The tokenizer confines a `--` to the hint body
                // and resumes at the matching `*/`; a flat count does too.
                //
                // Both ways of mis-locating that matching `*/` over-count, so
                // the mode is safe whatever nesting does. Stopping early
                // resumes `Normal` sooner and counts more text as top-level
                // structure. Running past it counts every remaining character.
                // Neither can admit a statement the tokenizer sees as deeper.
                if rest.starts_with("/*!") {
                    count += 3;
                    if count > stop_above {
                        return count;
                    }
                    skip = 2;
                    mode = Mode::Hint(1);
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

                // One token, one unit, whatever its length. A quoted literal
                // and a dollar-quoted body already cost one; an identifier,
                // keyword, or number run costs one for the same reason, since
                // the invariant this guard rests on is about tokens rather
                // than characters. Counting per character instead made the
                // bound depend on quoting: `"ResolutionWidth"` cost 1 and
                // `ResolutionWidth` cost 15, so the same statement passed or
                // failed on how its author quoted it, and a dashboard's
                // hundred-element numeric IN list was refused while a
                // four-hundred-element quoted one was admitted. The depth
                // argument is unchanged, because a loop-consumed chain still
                // spends one operand token plus one operator token per level.
                count += 1;
                if count > stop_above {
                    return count;
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
                    continue;
                }
                if c.is_alphanumeric() || c == '_' {
                    // Consume the rest of the run, stopping where the tokenizer
                    // would.
                    //
                    // A run that STARTS with a digit stops at the first
                    // non-digit, because `GenericDialect::supports_numeric_prefix`
                    // is false: `1AND` is `Number("1")` then `Word("AND")`, two
                    // tokens. Consuming it as one alphanumeric run charged one
                    // unit for two tokens, and since each `AND` adds a tree
                    // level that made a boolean chain cost one unit per level
                    // instead of two. Measured on the version that did:
                    // `SELECT 1` + `AND 1` x998 scored exactly 1,000 units and
                    // built a 998-level spine, against 499 for the same budget
                    // spent on `+1`. That is the whole calibration margin.
                    //
                    // A run that starts with a letter or `_` keeps consuming
                    // alphanumerics, because `a1` really is one identifier.
                    //
                    // A `.` inside a number (`1.5`, `1e-3`) is deliberately NOT
                    // consumed: it costs its own unit, which only makes the
                    // count stricter, never more permissive.
                    let run = if c.is_ascii_digit() {
                        rest.find(|ch: char| !ch.is_ascii_digit())
                            .unwrap_or(rest.len())
                    } else {
                        rest.find(|ch: char| !(ch.is_alphanumeric() || ch == '_'))
                            .unwrap_or(rest.len())
                    };
                    skip = rest[..run].chars().count() - 1;
                }
            }
        }
    }

    count
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

    /// `MAX_STATEMENT_COMPLEXITY - n` counted units, as that many separate
    /// `a` tokens. Space-separated because a run of `a` is ONE identifier
    /// token and therefore one unit, however long it is.
    fn filler(n: usize) -> String {
        "a ".repeat(MAX_STATEMENT_COMPLEXITY - n)
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

    /// A `/*!...*/` hint comment cannot hide an operator chain. This dialect
    /// re-tokenizes such a body into real tokens
    /// (`GenericDialect::supports_multiline_comment_hints` is true, and
    /// `Tokenizer` expands a `MultiLineComment` whose body opens with `!`), so
    /// a scan that skipped the region would admit exactly the flat chain this
    /// guard exists to refuse. Measured before the fix: this statement at
    /// n=2000 scored 7 while the tokenizer produced 4003 tokens from it.
    ///
    /// Flip to watch it fail: drop the `&& !rest.starts_with("/*!")` term from
    /// the block-comment arm in `scan`. The chain is skipped, the count falls
    /// to single digits, and the `expect_err` below panics.
    #[test]
    fn a_hint_comment_cannot_hide_an_operator_chain() {
        let chain = "+1".repeat(MAX_STATEMENT_COMPLEXITY);
        let sql = format!("SELECT 1/*!{chain}*/");
        let err = check(&sql).expect_err("a hint body is structure, not a comment");
        assert!(
            err.count > MAX_STATEMENT_COMPLEXITY,
            "the hint body must be counted, got {}",
            err.count
        );

        // The mirror: an ordinary block comment is still opaque, so the fix
        // did not simply stop skipping comments.
        let sql = format!("SELECT 1/*{chain}*/");
        assert!(check(&sql).is_ok(), "a plain block comment stays uncounted");
    }

    /// A sub-mode cannot carry an operator chain out of a hint body. The
    /// first fix for the hint bypass fell through to `Normal` inside the
    /// body, where `--` entered `LineComment` and ran to EOF, so
    /// `SELECT 1/*! -- */` plus a chain scored 5 while the tokenizer emitted
    /// 8005 tokens. Each opener below is the same argument through a
    /// different door, and the hint mode enters none of them.
    ///
    /// Flip to watch it fail: in `scan`'s `Normal` arm, replace the
    /// `Mode::Hint(1)` assignment with a `continue` that leaves `mode` as
    /// `Normal`. Every case below then scores single digits and is admitted.
    #[test]
    fn no_sub_mode_carries_a_chain_out_of_a_hint_body() {
        let chain = "+1".repeat(MAX_STATEMENT_COMPLEXITY);
        for opener in ["--", "'", "\"", "`", "$$", "/*"] {
            let sql = format!("SELECT 1/*! {opener} */{chain}");
            let err = check(&sql).expect_err("a hint body must not hide the chain");
            assert!(
                err.count > MAX_STATEMENT_COMPLEXITY,
                "`{opener}` inside a hint body hid the chain: counted {}",
                err.count
            );
        }
    }

    /// A digit run touching a following keyword costs two units, not one,
    /// because the tokenizer splits it into two tokens.
    ///
    /// `GenericDialect::supports_numeric_prefix()` is false, so `1AND` is
    /// `Number("1")` then `Word("AND")`. An earlier form of the run rule
    /// consumed it as one alphanumeric run and charged one unit, and since each
    /// `AND` adds a tree level that made a boolean chain cost one unit per level
    /// against the two every other construct costs. Measured on that form:
    /// `SELECT 1` + `AND 1` x998 scored exactly 1,000 units and built a
    /// 998-level spine, where the same budget spent on `+1` reaches 499. The
    /// calibration argument this guard rests on is that no construct costs
    /// fewer than two units per level, so that one construct falsified it and
    /// put an admitted statement within about 5% of the measured abort.
    ///
    /// Flip to watch it fail: drop the `c.is_ascii_digit()` branch from the run
    /// consumption in `scan`, leaving the single alphanumeric arm. The `AND`
    /// chain below then scores 1,000 and is accepted.
    #[test]
    fn a_digit_touching_a_keyword_costs_two_units() {
        // 998 levels' worth of `AND`, which the one-unit rule admitted.
        let sql = format!("SELECT 1{}", "AND 1".repeat(998));
        let err = check(&sql).expect_err("a boolean chain must cost two units per level");
        assert!(
            err.count > MAX_STATEMENT_COMPLEXITY,
            "the AND chain must be refused, counted {}",
            err.count
        );

        // The two chains now cost the same per level, which is the property
        // the calibration rests on.
        assert_eq!(
            structural_count("SELECT 1AND 1AND 1"),
            structural_count("SELECT 1+1+1"),
            "a boolean chain and an arithmetic chain of equal depth cost equally"
        );

        // And an identifier run is still one unit, so the quoting independence
        // the token rule exists for is intact.
        assert_eq!(
            structural_count("SELECT ResolutionWidth FROM t"),
            structural_count("SELECT a1 FROM t"),
        );
    }

    /// One token costs one unit whatever its length, so the bound does not
    /// depend on how an author quoted an identifier. Before this rule a
    /// hundred-element numeric IN list was refused while a four-hundred-element
    /// quoted one was admitted, though the numeric one is the shallower tree.
    ///
    /// Flip to watch it fail: delete the identifier-run `skip` at the end of
    /// `scan`'s `Normal` arm. Each character of every bare identifier and
    /// number is then counted again and the numeric list below is rejected.
    #[test]
    fn a_token_costs_one_unit_whatever_its_length() {
        let quoted = std::iter::repeat_n("\'series-0001\'", 100)
            .collect::<Vec<_>>()
            .join(", ");
        let numeric = std::iter::repeat_n("80231457", 100)
            .collect::<Vec<_>>()
            .join(", ");

        let q = structural_count(&format!("SELECT v FROM t WHERE id IN ({quoted})"));
        let n = structural_count(&format!("SELECT v FROM t WHERE id IN ({numeric})"));
        assert_eq!(
            q, n,
            "the same hundred-element list must cost the same quoted or bare"
        );
        assert!(
            n <= MAX_STATEMENT_COMPLEXITY,
            "a hundred-element IN list must fit the bound, got {n}"
        );

        // A long bare identifier costs the same as a short one.
        assert_eq!(
            structural_count("SELECT ResolutionWidth FROM t"),
            structural_count("SELECT w FROM t"),
            "identifier length must not change the cost"
        );
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
        let at = "a ".repeat(MAX_STATEMENT_COMPLEXITY);
        assert_eq!(check(&at), Ok(()));
        let over = "a ".repeat(MAX_STATEMENT_COMPLEXITY + 1);
        assert_eq!(
            check(&over),
            Err(StatementTooComplex {
                count: MAX_STATEMENT_COMPLEXITY + 1,
                max: MAX_STATEMENT_COMPLEXITY,
            })
        );
    }

    /// Whitespace is free, so a statement formatted across many lines is not
    /// penalised for its formatting: the same seven units
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
