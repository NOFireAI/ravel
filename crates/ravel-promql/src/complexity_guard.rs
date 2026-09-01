//! Pre-parse structural-complexity guard for raw PromQL query text (issue
//! #529).
//!
//! `promql_parser::parser::parse` is a hand-written recursive-descent
//! parser with no depth guard of its own, and it recurses at least once per
//! structural token it consumes (a paren, a bracket, a unary operator, a
//! binary/logical operator). A query built to maximize any one of those
//! aborts the process with a stack overflow *during parsing*, before any
//! AST exists to walk — which rules out a post-parse `check_ast`-style
//! guard as the sole defense: by the time an AST is available to inspect,
//! the crash has already happened. This module scans the raw query text
//! instead, so the reject happens before `promql_parser::parser::parse` is
//! ever called.
//!
//! The bound enforced is deliberately not "nesting depth": an early design
//! of this guard counted only bracket depth and consecutive unary-operator
//! runs, and was proven insufficient by direct experiment — a flat chain of
//! binary operators with no brackets at all (`1+1+1+...+1`) and a flat
//! chain of `and` (`up and up and ... and up`) both crash the parser at a
//! similar depth to the bracket/unary cases, because the parser recurses
//! once per operator regardless of whether the query text nests visually.
//! Rather than enumerate every construct that can trigger recursion (a
//! whack-a-mole that would remain unsound against a construct nobody
//! thought to test), this guard bounds a simpler, sound invariant: a
//! recursive-descent parser cannot recurse more times than it has
//! structural characters to consume, so capping
//! `count(non-whitespace chars outside string literals and comments)`
//! caps parse (and any downstream AST-walk) depth by construction, for
//! every construct, tested or not.
//!
//! String literals (`'...'`/`"..."`, backslash-escaped) and line comments
//! (`#` to end of line) are excluded from the count on purpose: a label
//! matcher's regex value (`job=~"a|b|c|...|zzz"`) can legitimately be very
//! long without adding any parser recursion, since matcher values are
//! opaque string tokens to the parser, not re-parsed structure.
//!
//! # Calibrating [`MAX_QUERY_COMPLEXITY`]
//!
//! Measured empirically against `promql-parser` 0.10.0 on a 2 MiB stack
//! (tokio's worker-thread default, which this crate's evaluator runs on),
//! via a standalone probe outside the workspace build:
//!
//! | construct                          | chars/level | survives | aborts |
//! |-------------------------------------|:-----------:|:--------:|:------:|
//! | `(((...up...)))`  (paren nesting)   | 2           | 32,000   | 34,000 |
//! | `-------...up`    (unary run)       | 1           | 30,000   | 40,000 |
//! | `1+1+1+...+1`     (binary chain)    | 2           | 30,000   | 35,000 |
//! | `up and up and...`(logical chain)   | ~7          | 30,000   | 40,000 |
//!
//! The tightest floor in raw character count is the unary case (survives
//! at 30,000 characters). [`MAX_QUERY_COMPLEXITY`] sits about 60x under
//! that floor, which comfortably absorbs any difference between this
//! probe's frame size and a release build's, and between a parse frame and
//! an `eval_expr` frame, without needing to prove those sizes match.
//!
//! A single matcher list (`up{a="1",b="2",...}`) is not itself a
//! recursion risk — confirmed by probing a 100,000-matcher selector, which
//! parses without incident (the parser builds it as a flat `Vec`, not a
//! recursive structure) — but this guard does not special-case brace
//! interiors: an adversarial, syntactically-invalid payload inside `{...}`
//! has not been verified safe from triggering the parser's recursive
//! error-recovery path, and the whole point of the sound-invariant
//! approach above is to not depend on a per-construct safety argument.
//! [`MAX_QUERY_COMPLEXITY`] is sized to still comfortably fit a real
//! query's non-string structure (function calls, `by`/`without` clauses,
//! a few dozen matcher names/operators) even without that exclusion.

use std::fmt;

/// Maximum count of non-whitespace characters outside string literals and
/// line comments a query's text may contain. See the module documentation
/// for how this was measured and why it bounds parser (and AST-walk)
/// recursion depth for every construct, not just the ones tested.
pub const MAX_QUERY_COMPLEXITY: usize = 500;

/// `query`'s structural character count exceeded [`MAX_QUERY_COMPLEXITY`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTooComplex {
    /// The count at which the scan stopped (one past the max).
    pub count: usize,
    /// [`MAX_QUERY_COMPLEXITY`], carried alongside the measured count so
    /// callers can report both without reaching back into this module.
    pub max: usize,
}

impl fmt::Display for QueryTooComplex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query complexity {} exceeds the maximum of {} structural characters; simplify the expression",
            self.count, self.max
        )
    }
}

impl std::error::Error for QueryTooComplex {}

/// Scans `query`'s raw text for excessive structural complexity, outside
/// string literals and line comments. Call this before
/// `promql_parser::parser::parse`, not after: the crash this guards
/// against happens during parsing itself.
pub fn check(query: &str) -> Result<(), QueryTooComplex> {
    #[derive(PartialEq)]
    enum Mode {
        Normal,
        Single,
        Double,
        Comment,
    }

    let mut mode = Mode::Normal;
    let mut escaped = false;
    let mut count: usize = 0;

    for c in query.chars() {
        match mode {
            Mode::Comment => {
                if c == '\n' {
                    mode = Mode::Normal;
                }
                continue;
            }
            Mode::Single | Mode::Double => {
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if (mode == Mode::Single && c == '\'') || (mode == Mode::Double && c == '"')
                {
                    mode = Mode::Normal;
                }
                continue;
            }
            Mode::Normal => {}
        }

        match c {
            '\'' => mode = Mode::Single,
            '"' => mode = Mode::Double,
            '#' => mode = Mode::Comment,
            c if c.is_whitespace() => {}
            _ => {
                count += 1;
                if count > MAX_QUERY_COMPLEXITY {
                    return Err(QueryTooComplex {
                        count,
                        max: MAX_QUERY_COMPLEXITY,
                    });
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_query_passes() {
        assert!(check(r#"rate(http_requests_total{job="api"}[5m])"#).is_ok());
    }

    #[test]
    fn deep_paren_nesting_rejected() {
        let query = format!("{}{}{}", "(".repeat(400), "up", ")".repeat(400));
        let err = check(&query).expect_err("must be rejected");
        assert_eq!(err.max, MAX_QUERY_COMPLEXITY);
        assert!(err.count > MAX_QUERY_COMPLEXITY);
    }

    #[test]
    fn deep_unary_run_rejected() {
        let query = format!("{}up", "-".repeat(600));
        assert!(check(&query).is_err());
    }

    #[test]
    fn flat_binary_chain_rejected() {
        // No brackets and no unary run at all: the case that proved a
        // bracket/unary-only guard insufficient.
        let mut query = String::from("1");
        for _ in 0..600 {
            query.push_str("+1");
        }
        assert!(check(&query).is_err());
    }

    #[test]
    fn flat_logical_chain_rejected() {
        let mut query = String::from("up");
        for _ in 0..200 {
            query.push_str(" and up");
        }
        assert!(check(&query).is_err());
    }

    #[test]
    fn error_message_names_complexity_not_nesting() {
        // A flat chain has no nesting at all; the message must not claim
        // "depth" or "nesting" for a rejection that fires on flatness.
        let mut query = String::from("1");
        for _ in 0..600 {
            query.push_str("+1");
        }
        let err = check(&query).expect_err("must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("complexity"), "{msg}");
        assert!(!msg.contains("nest"), "{msg}");
        assert!(!msg.contains("depth"), "{msg}");
    }

    #[test]
    fn long_regex_matcher_value_not_counted() {
        // The regex payload lives in a quoted string; it must not
        // contribute to the structural count no matter how long it is.
        let alternation = (0..300)
            .map(|i| format!("v{i}"))
            .collect::<Vec<_>>()
            .join("|");
        let query = format!(r#"up{{job=~"{alternation}"}}"#);
        assert!(check(&query).is_ok());
    }

    #[test]
    fn moderate_flat_matcher_list_passes() {
        let matchers: Vec<String> = (0..50).map(|i| format!("l{i}=\"v\"")).collect();
        let query = format!("up{{{}}}", matchers.join(","));
        assert!(check(&query).is_ok());
    }

    #[test]
    fn paren_nesting_inside_comment_ignored() {
        let query = format!("# {}\nup", "(".repeat(400));
        assert!(check(&query).is_ok());
    }

    #[test]
    fn escaped_quote_inside_string_does_not_end_it() {
        // A closing paren right after an escaped quote must still be seen
        // as inside the string, not as the start of fresh structure.
        let query = format!(r#"up{{job="a\"{}"}}"#, "(".repeat(400));
        assert!(check(&query).is_ok());
    }
}
