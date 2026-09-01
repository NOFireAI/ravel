//! AST-level keyed tokenization of PromQL query text (ADR-0062 decision 2e).
//!
//! The audit trail's default posture stores a *structure-preserving redacted*
//! form of every query: the parsed query re-rendered with each PII-carrying
//! value replaced by a deterministic keyed token, while selector names, label
//! names, operators, functions, and the query's shape stay readable. That
//! shows exactly what shape of query ran without persisting the low-entropy
//! identifiers (emails, user IDs, IPs) that live in label-matcher values.
//!
//! [`redact`] parses `query` with the same `promql_parser::parser::parse`
//! entry point the evaluator uses, walks the resulting [`Expr`] tree in place,
//! replaces every redactable value with a token, and re-renders the mutated
//! tree through the parser's own `Display` impl. `promql_parser` renders a
//! label matcher as `name <op> "value"` from the matcher's `op` and `value`
//! fields alone (the compiled regex inside `MatchOp::Re`/`NotRe` is never
//! consulted by `Display`), so replacing the `value` string is sufficient to
//! redact a `=~`/`!~` matcher and the re-rendered text still re-parses.
//!
//! What is redacted, and what stays readable:
//!
//! - **Label-matcher values** (`job="api"` -> `job="tok_..."`) for every
//!   operator, including the alternative (`or`-grouped) matchers. The label
//!   *name* and operator are untouched.
//! - The `__name__` matcher value is left readable: it *is* the selector
//!   name, which the ADR keeps readable. (The common `up{...}` spelling puts
//!   the metric name in `VectorSelector::name`, not in a matcher, so this only
//!   matters for the explicit `{__name__="up"}` form.)
//! - **String literals** (`label_replace(v, "dst", "$1", "src", "(.*)")`) are
//!   tokenized: they are ordinary strings and can carry PII, and a token
//!   re-renders as a valid quoted string operand.
//! - **Numeric literals are left readable and are *not* tokenized.** This is a
//!   deliberate scope decision, not an oversight. A PromQL number is a bare
//!   token in the grammar (`up > 5`, `rate(x[5m])`), so replacing it with a
//!   prefixed string token would either fail to re-parse or silently change
//!   the query's structure (`up > 5`, a vector/scalar comparison, would become
//!   `up > tok_...`, a vector/vector comparison). The ADR's PII rationale
//!   names "label-matcher values and SQL string literals" as the identifier
//!   carriers; a PromQL numeric threshold or duration is structural, not an
//!   identifier, so keeping it readable both preserves the query shape and
//!   satisfies the re-parseability requirement. Durations in matrix selectors
//!   (`[5m]`) are likewise structural and untouched.
//!
//! The token generator [`audit_token`] lives here and is reused by
//! `ravel-sql`'s equivalent SQL redactor (`ravel-sql` already depends on
//! `ravel-promql`), so both surfaces emit the identical token shape from the
//! identical keyed-hash scheme.

use promql_parser::label::Matchers;
use promql_parser::parser::{self, Expr};

/// The fixed prefix every token carries, so a redacted value is visually
/// distinguishable from a real label value at a glance.
const TOKEN_PREFIX: &str = "tok_";

/// Number of leading hex characters of the keyed hash kept in a token. 16 hex
/// chars is 64 bits: collision-resistant enough for the audit trail's
/// correlation use while keeping records compact.
const TOKEN_HEX_LEN: usize = 16;

/// The reserved metric-name label. Its value is the selector name, which the
/// ADR keeps readable, so it is never tokenized.
const METRIC_NAME_LABEL: &str = "__name__";

/// A failure to redact a query. The only failure modes are the underlying
/// parser rejecting malformed input and the pre-parse complexity guard
/// ([`crate::complexity_guard`]) rejecting an excessively complex one; the
/// walk and re-render are infallible.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedactError {
    /// `query` did not parse as PromQL, or was rejected before parsing for
    /// exceeding the complexity guard (issue #529): a malformed audit-log
    /// query and a maliciously deep one are both PromQL text this module
    /// cannot turn into a redacted record.
    ///
    /// Carries no parser detail on purpose: `promql_parser`'s own error
    /// message echoes fragments of the offending input verbatim (an illegal
    /// regex message quotes the literal pattern; a duplicate-label message
    /// quotes the label value), which can be exactly the PII this module
    /// exists to redact. Storing or logging that message would leak it, so
    /// the error is a fixed, input-independent label.
    #[error("PromQL parse error")]
    Parse,
}

/// The deterministic audit token for `value` under `token_key`:
/// `blake3::keyed_hash(token_key, value)` hex-encoded, truncated to
/// [`TOKEN_HEX_LEN`], and prefixed with [`TOKEN_PREFIX`].
///
/// Deterministic by construction: the same `value` under the same
/// `token_key` always yields the same token, and a different key yields a
/// different token for the same value (the key is load-bearing, per the ADR's
/// keyed-hashing requirement, so low-entropy values are not recoverable by an
/// offline dictionary attack). Shared with `ravel-sql` so the SQL and PromQL
/// surfaces emit the identical token shape.
pub fn audit_token(token_key: &[u8; 32], value: &[u8]) -> String {
    let hash = blake3::keyed_hash(token_key, value);
    let hex = hash.to_hex();
    format!("{TOKEN_PREFIX}{}", &hex[..TOKEN_HEX_LEN])
}

/// Parse `query`, replace every label-matcher value and string literal with a
/// deterministic keyed token, and re-render the result to PromQL text.
///
/// The output re-parses as valid PromQL: only matcher values and string
/// operands change, and only into other valid values. See the module docs for
/// the exact redaction rules (and why numeric literals stay readable).
pub fn redact(query: &str, token_key: &[u8; 32]) -> Result<String, RedactError> {
    crate::complexity_guard::check(query).map_err(|_| RedactError::Parse)?;
    let mut expr = parser::parse(query).map_err(|_| RedactError::Parse)?;
    redact_expr(&mut expr, token_key);
    Ok(expr.to_string())
}

/// Recursively redact every redactable value reachable from `expr`, in place.
fn redact_expr(expr: &mut Expr, key: &[u8; 32]) {
    match expr {
        Expr::Aggregate(a) => {
            redact_expr(&mut a.expr, key);
            if let Some(param) = a.param.as_deref_mut() {
                redact_expr(param, key);
            }
        }
        Expr::Unary(u) => redact_expr(&mut u.expr, key),
        Expr::Binary(b) => {
            redact_expr(&mut b.lhs, key);
            redact_expr(&mut b.rhs, key);
        }
        Expr::Paren(p) => redact_expr(&mut p.expr, key),
        Expr::Subquery(s) => redact_expr(&mut s.expr, key),
        Expr::Call(c) => {
            for arg in &mut c.args.args {
                redact_expr(arg, key);
            }
        }
        Expr::StringLiteral(s) => {
            s.val = audit_token(key, s.val.as_bytes());
        }
        // Numeric literals stay readable (see module docs): tokenizing a bare
        // PromQL number would break re-parse or change query structure.
        Expr::NumberLiteral(_) => {}
        Expr::VectorSelector(vs) => redact_matchers(&mut vs.matchers, key),
        Expr::MatrixSelector(ms) => redact_matchers(&mut ms.vs.matchers, key),
        // The parser never produces `Extension`; nothing to redact.
        Expr::Extension(_) => {}
    }
}

/// Redact every matcher value in `matchers` (plain and `or`-grouped),
/// preserving label names, operators, and the readable `__name__` value.
fn redact_matchers(matchers: &mut Matchers, key: &[u8; 32]) {
    for m in &mut matchers.matchers {
        redact_matcher_value(m, key);
    }
    for group in &mut matchers.or_matchers {
        for m in group {
            redact_matcher_value(m, key);
        }
    }
}

/// Replace one matcher's value with a token, unless it is the `__name__`
/// matcher (whose value is the selector name and stays readable).
fn redact_matcher_value(m: &mut promql_parser::label::Matcher, key: &[u8; 32]) {
    if m.name == METRIC_NAME_LABEL {
        return;
    }
    m.value = audit_token(key, m.value.as_bytes());
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const KEY_A: [u8; 32] = [1u8; 32];
    const KEY_B: [u8; 32] = [2u8; 32];

    fn is_token(s: &str) -> bool {
        s.starts_with(TOKEN_PREFIX) && s.len() == TOKEN_PREFIX.len() + TOKEN_HEX_LEN
    }

    #[test]
    fn label_matcher_values_tokenized_structure_preserved() {
        let out = redact(r#"up{job="api", env=~"prod.*"}"#, &KEY_A).expect("redacts");

        // Selector name, label names, and operators are readable and intact.
        assert!(out.starts_with("up{"), "selector name preserved: {out}");
        assert!(
            out.contains("job="),
            "job label name + eq op preserved: {out}"
        );
        assert!(
            out.contains("env=~"),
            "env label name + regex op preserved: {out}"
        );

        // The original values are gone, replaced by tokens.
        assert!(!out.contains("api"), "raw value leaked: {out}");
        assert!(!out.contains("prod"), "raw value leaked: {out}");

        // The redacted output is valid PromQL again.
        promql_parser::parser::parse(&out).expect("redacted output re-parses");
    }

    #[test]
    fn same_value_same_key_is_deterministic() {
        let a = redact(r#"up{job="api"}"#, &KEY_A).expect("redacts");
        let b = redact(r#"up{job="api"}"#, &KEY_A).expect("redacts");
        assert_eq!(a, b, "same value under same key must tokenize identically");
    }

    #[test]
    fn same_value_across_matchers_tokenizes_identically() {
        // Determinism gives correlation: the same value in two matchers must
        // produce the same token, so an investigator can trace it.
        let out = redact(r#"up{job="api"} + down{svc="api"}"#, &KEY_A).expect("redacts");
        let token = audit_token(&KEY_A, b"api");
        let occurrences = out.matches(&token).count();
        assert_eq!(occurrences, 2, "equal values must share a token: {out}");
    }

    #[test]
    fn different_key_produces_different_token() {
        let a = redact(r#"up{job="api"}"#, &KEY_A).expect("redacts");
        let b = redact(r#"up{job="api"}"#, &KEY_B).expect("redacts");
        assert_ne!(a, b, "the token key must be load-bearing");
    }

    #[test]
    fn token_shape_is_prefixed_and_fixed_length() {
        let t = audit_token(&KEY_A, b"whatever");
        assert!(is_token(&t), "unexpected token shape: {t}");
    }

    #[test]
    fn string_literals_are_tokenized() {
        let out = redact(
            r#"label_replace(up, "dst", "keepme", "src", "(.*)")"#,
            &KEY_A,
        )
        .expect("redacts");
        assert!(
            !out.contains("keepme"),
            "string literal value leaked: {out}"
        );
        // Function name and structure stay readable.
        assert!(out.contains("label_replace("), "function preserved: {out}");
        promql_parser::parser::parse(&out).expect("redacted output re-parses");
    }

    #[test]
    fn numeric_literals_stay_readable() {
        // Deliberate scope decision (see module docs): a bare PromQL number is
        // not tokenized, both to keep the output re-parseable and because a
        // numeric threshold is structural, not an identifier.
        let out = redact("up > 5", &KEY_A).expect("redacts");
        assert!(
            out.contains('5'),
            "numeric threshold should stay readable: {out}"
        );
        assert!(!out.contains(TOKEN_PREFIX), "no token expected: {out}");
        promql_parser::parser::parse(&out).expect("redacted output re-parses");
    }

    #[test]
    fn query_without_values_is_unchanged() {
        // `up` alone has no matcher values or literals: no spurious tokens.
        let out = redact("up", &KEY_A).expect("redacts");
        assert_eq!(out, "up");
        assert!(!out.contains(TOKEN_PREFIX));
    }

    #[test]
    fn metric_name_matcher_value_stays_readable() {
        // The explicit `__name__` matcher carries the selector name, which the
        // ADR keeps readable; only the real label value is tokenized.
        let out = redact(r#"{__name__="up", job="api"}"#, &KEY_A).expect("redacts");
        assert!(
            out.contains("up"),
            "metric name should stay readable: {out}"
        );
        assert!(
            !out.contains("api"),
            "label value should be tokenized: {out}"
        );
        promql_parser::parser::parse(&out).expect("redacted output re-parses");
    }

    #[test]
    fn malformed_input_returns_typed_error_without_panic() {
        let err = redact("up{job=", &KEY_A).expect_err("must reject malformed input");
        assert!(matches!(err, RedactError::Parse));
    }

    /// Issue #529: the audit trail calls `redact` on every query it
    /// records, so it needs the same pre-parse complexity guard the
    /// evaluator has, not just a well-formed-syntax check.
    #[test]
    fn overly_complex_query_returns_typed_error_without_panic() {
        let mut query = String::from("1");
        for _ in 0..300 {
            query.push_str("+1");
        }
        let err = redact(&query, &KEY_A).expect_err("must reject an overly complex query");
        assert!(matches!(err, RedactError::Parse));
    }

    #[test]
    fn error_display_never_leaks_input_literals() {
        // The raw parser message DOES quote the offending literal verbatim:
        // this documents the leak the fixed-label Parse error closes. An
        // illegal regex in a `=~` matcher makes promql_parser echo the
        // pattern text in its own error message.
        let probe = r#"up{user=~"[alice@example.com"}"#;
        let raw = parser::parse(probe)
            .expect_err("malformed input")
            .to_string();
        assert!(
            raw.contains("alice@example.com"),
            "raw parser message unexpectedly safe, rationale stale: {raw}"
        );

        // Our Parse error carries none of it.
        let err = redact(probe, &KEY_A).expect_err("rejected");
        assert!(matches!(err, RedactError::Parse));
        assert!(
            !err.to_string().contains("alice@example.com"),
            "Parse Display leaked a literal: {err}"
        );
    }
}
