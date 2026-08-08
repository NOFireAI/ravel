//! AST-level keyed tokenization of SQL query text (ADR-0062 decision 2e,
//! epic EL / issue #761).
//!
//! The SQL counterpart of `ravel_promql::redact`: the audit trail's default
//! posture stores a structure-preserving redacted form of every query, with
//! each literal value replaced by a deterministic keyed token while column
//! names, table names, operators, keywords, and structure stay readable.
//!
//! [`redact`] parses `query` with the same `DFParser::parse_sql` front end the
//! read-only gate ([`crate::validate`]) uses, walks the parsed statement with
//! sqlparser's `VisitorMut` (the `visitor` feature is already enabled on
//! `sqlparser` via `datafusion-sql`, and this crate already uses its immutable
//! `Visitor` in `validate`), replaces every literal `Value` with a token, and
//! re-renders through the AST's own `Display` impl.
//!
//! The token generator is shared with the PromQL side: [`redact`] calls
//! `ravel_promql::redact::audit_token`, which lives in `ravel-promql` (a crate
//! `ravel-sql` already depends on). Both surfaces therefore emit the identical
//! `tok_<hex>` shape from the identical `blake3::keyed_hash` scheme, so equal
//! values tokenize equally across the whole audit trail regardless of which
//! query language produced them.
//!
//! What is redacted, and what stays readable:
//!
//! - **String literals and numeric literals** (`WHERE user = 'alice' AND age >
//!   30`) are tokenized. Unlike PromQL, a SQL numeric literal *can* be
//!   tokenized while keeping the output re-parseable: it is replaced with a
//!   single-quoted string token, and sqlparser accepts a string literal
//!   anywhere an expression is expected, so `age > 'tok_...'` still parses. A
//!   SQL numeric literal (an account number, an id) is as much an identifier
//!   carrier as a string, so tokenizing it matches the ADR's PII intent.
//! - **`NULL`, boolean literals, and placeholders** (`$1`, `?`) are left
//!   readable: they carry no identifier and are structural.
//! - Column names, table names, function names, operators, and keywords are
//!   never touched: only `Value` nodes are visited.

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Statement, Value, ValueWithSpan, VisitMut, VisitorMut};
use ravel_promql::audit_token;
use std::ops::ControlFlow;

/// A failure to redact a SQL query. The only failure mode is the parser
/// rejecting malformed input; the walk and re-render are infallible.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedactError {
    /// `query` did not parse. The message is the parser's own, which quotes
    /// only the caller's input.
    #[error("SQL parse error: {0}")]
    Parse(String),
}

/// Parse `query`, replace every literal value with a deterministic keyed
/// token, and re-render the result to SQL text.
///
/// The output re-parses as valid SQL: every literal becomes a single-quoted
/// string token, which is a valid operand anywhere a literal was. See the
/// module docs for the exact redaction rules.
pub fn redact(query: &str, token_key: &[u8; 32]) -> Result<String, RedactError> {
    let mut statements =
        DFParser::parse_sql(query).map_err(|e| RedactError::Parse(strip_prefix(&e.to_string())))?;

    let mut redactor = LiteralRedactor { key: token_key };
    let mut rendered = Vec::with_capacity(statements.len());
    for statement in statements.iter_mut() {
        if let DFStatement::Statement(inner) = statement {
            let stmt: &mut Statement = inner;
            let _ = VisitMut::visit(stmt, &mut redactor);
        }
        // Non-`Statement` DFParser extensions (CREATE EXTERNAL TABLE, COPY,
        // EXPLAIN, RESET) are rejected by the read-only gate before they ever
        // reach the audit path; redact leaves them structurally intact rather
        // than reaching into value-free DDL, and re-renders them unchanged.
        rendered.push(statement.to_string());
    }
    Ok(rendered.join("; "))
}

/// Visits every `Value` node in a statement and replaces each redactable
/// literal with a keyed token, in place.
struct LiteralRedactor<'a> {
    key: &'a [u8; 32],
}

impl VisitorMut for LiteralRedactor<'_> {
    type Break = ();

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
        if let Some(text) = redactable_content(&value.value) {
            value.value = Value::SingleQuotedString(audit_token(self.key, text.as_bytes()));
        }
        ControlFlow::Continue(())
    }
}

/// The canonical text to tokenize for `value`, or `None` if `value` is not a
/// redactable literal (NULL, boolean, or placeholder: structural, no
/// identifier). String literals hash their inner content; numbers hash their
/// rendered form (feature-agnostic across sqlparser's `bigdecimal` flag).
fn redactable_content(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Boolean(_) | Value::Placeholder(_) => None,
        Value::Number(..) => Some(value.to_string()),
        other => Some(
            other
                .clone()
                .into_string()
                .unwrap_or_else(|| other.to_string()),
        ),
    }
}

/// sqlparser prefixes its errors with "SQL error: "; drop the duplicate, the
/// same normalization [`crate::validate`] applies.
fn strip_prefix(msg: &str) -> String {
    msg.strip_prefix("SQL error: ")
        .unwrap_or(msg)
        .trim()
        .to_string()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::sql::parser::DFParser;

    const KEY_A: [u8; 32] = [1u8; 32];
    const KEY_B: [u8; 32] = [2u8; 32];

    fn reparse(sql: &str) {
        DFParser::parse_sql(sql).expect("redacted output re-parses as valid SQL");
    }

    #[test]
    fn where_literals_tokenized_structure_preserved() {
        let out = redact(
            "SELECT value FROM samples WHERE service = 'checkout' AND value > 30",
            &KEY_A,
        )
        .expect("redacts");

        // Column names, table name, and keywords stay readable.
        assert!(out.contains("samples"), "table name preserved: {out}");
        assert!(out.contains("service"), "column name preserved: {out}");
        assert!(out.contains("value"), "column name preserved: {out}");

        // Literal values are gone.
        assert!(!out.contains("checkout"), "string literal leaked: {out}");
        assert!(!out.contains("30"), "numeric literal leaked: {out}");
        assert!(out.contains("tok_"), "tokens expected: {out}");

        reparse(&out);
    }

    #[test]
    fn same_value_same_key_is_deterministic() {
        let a = redact("SELECT * FROM t WHERE a = 'x'", &KEY_A).expect("redacts");
        let b = redact("SELECT * FROM t WHERE a = 'x'", &KEY_A).expect("redacts");
        assert_eq!(a, b, "same value under same key must tokenize identically");
    }

    #[test]
    fn different_key_produces_different_token() {
        let a = redact("SELECT * FROM t WHERE a = 'x'", &KEY_A).expect("redacts");
        let b = redact("SELECT * FROM t WHERE a = 'x'", &KEY_B).expect("redacts");
        assert_ne!(a, b, "the token key must be load-bearing");
    }

    #[test]
    fn token_matches_promql_side_generator() {
        // The shared generator means a value tokenizes identically whether it
        // came through the SQL or the PromQL surface.
        let out = redact("SELECT * FROM t WHERE a = 'shared'", &KEY_A).expect("redacts");
        let token = ravel_promql::audit_token(&KEY_A, b"shared");
        assert!(
            out.contains(&token),
            "expected shared token {token} in {out}"
        );
    }

    #[test]
    fn query_without_literals_introduces_no_tokens() {
        let out = redact("SELECT * FROM t", &KEY_A).expect("redacts");
        assert!(!out.contains("tok_"), "no spurious tokens: {out}");
        reparse(&out);
    }

    #[test]
    fn null_and_boolean_stay_readable() {
        let out =
            redact("SELECT * FROM t WHERE a IS NOT NULL AND b = true", &KEY_A).expect("redacts");
        assert!(out.to_uppercase().contains("NULL"), "NULL preserved: {out}");
        assert!(
            out.to_lowercase().contains("true"),
            "boolean preserved: {out}"
        );
        reparse(&out);
    }

    #[test]
    fn malformed_input_returns_typed_error_without_panic() {
        let err = redact("SELECT * FROM t WHERE", &KEY_A).expect_err("must reject malformed input");
        assert!(matches!(err, RedactError::Parse(_)));
    }
}
