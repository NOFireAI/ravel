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
//! - **String literals and identifying numeric literals** (`WHERE user =
//!   'alice' AND age > 30`) are tokenized. Unlike PromQL, a SQL numeric literal
//!   *can* be tokenized while keeping the output re-parseable: it is replaced
//!   with a single-quoted string token, and sqlparser accepts a string literal
//!   anywhere an expression is expected, so `age > 'tok_...'` still parses. A
//!   SQL numeric literal in a value position (an account number, an id) is as
//!   much an identifier carrier as a string, so tokenizing it matches the ADR's
//!   PII intent.
//! - **Structural numeric literals stay readable.** A number that shapes the
//!   query rather than naming a subject is not an identifier and is left
//!   verbatim: the `LIMIT`/`OFFSET`/`FETCH` counts and a bare positional
//!   `ORDER BY` ordinal (`ORDER BY 1`). Tokenizing these would both destroy
//!   operationally useful information (`LIMIT 10` and `LIMIT 1000000` become
//!   indistinguishable) and, for a positional `ORDER BY`, change the query's
//!   meaning (an ordinal turned into a string literal is no longer an ordinal).
//!   This mirrors the PromQL side, where durations and thresholds stay
//!   readable. The carve-out is narrow: it covers only those four structural
//!   positions. A literal anywhere else -- `WHERE`, `HAVING`, `JOIN ... ON`,
//!   function arguments, projection, `IN (...)`, `CASE`, CTEs -- still
//!   tokenizes, including a value hidden inside an `ORDER BY` expression that
//!   is not a bare ordinal (e.g. `ORDER BY CASE WHEN a = 'x' THEN 1 END`).
//! - **`NULL`, boolean literals, and placeholders** (`$1`, `?`) are left
//!   readable: they carry no identifier and are structural.
//! - Column names, table names, function names, operators, and keywords are
//!   never touched: only `Value` nodes are visited.
//!
//! **Unsupported statement shapes fail loudly.** `DFParser` parses four
//! non-`Statement` extensions -- `CREATE EXTERNAL TABLE`, `COPY`, `EXPLAIN`,
//! and `RESET`. These are rejected by the read-only gate ([`crate::validate`])
//! with `QueryStatus::Error`, but the audit path is still invoked with the raw
//! request text on that error branch, so they *do* reach redaction. This
//! redactor does not know how to walk their value positions (an `EXPLAIN` in
//! particular wraps a full inner statement whose `WHERE` literals would
//! otherwise re-render verbatim), so rather than pass unredacted text through
//! it returns [`RedactError::Unsupported`] for the whole call. A redaction
//! primitive that silently echoes unredacted text on an input shape it does
//! not handle is worse than one that fails: the caller must never store text
//! this function did not actually redact.

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr, LimitClause, OrderByKind, Query, Statement, Value, ValueWithSpan, VisitMut, VisitorMut,
};
use ravel_promql::audit_token;
use std::ops::ControlFlow;

/// A failure to redact a SQL query.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RedactError {
    /// `query` did not parse as SQL.
    ///
    /// Carries no parser detail on purpose (F3): sqlparser's own error message
    /// quotes the offending token verbatim, and that token can be a caller
    /// literal (`SELECT * FROM 'alice'` yields `... found: 'alice' ...`).
    /// Storing or logging such a message would leak exactly the PII this module
    /// exists to redact, so the error is a fixed, input-independent label.
    #[error("SQL parse error")]
    Parse,

    /// `query` parsed to a statement shape this redactor does not walk (a
    /// `DFParser` extension: `CREATE EXTERNAL TABLE`, `COPY`, `EXPLAIN`, or
    /// `RESET`). `kind` is a fixed static label for the shape; it is never
    /// derived from the input text, so it cannot carry a caller value.
    #[error("unsupported SQL statement for redaction: {kind}")]
    Unsupported { kind: &'static str },
}

/// Parse `query`, replace every identifying literal with a deterministic keyed
/// token, and re-render the result to SQL text.
///
/// The output re-parses as valid SQL: every tokenized literal becomes a
/// single-quoted string token, which is a valid operand anywhere a literal was.
/// See the module docs for the exact redaction rules.
///
/// # Errors
///
/// Returns [`RedactError::Parse`] if `query` does not parse, and
/// [`RedactError::Unsupported`] if any parsed statement is not a plain SQL
/// `Statement` (an `EXPLAIN`/`COPY`/`CREATE EXTERNAL TABLE`/`RESET` extension).
/// The whole call fails on the first such statement: a multi-statement input
/// is never partially redacted, because returning the redacted prefix of a
/// batch whose remainder went unredacted would still leak.
pub fn redact(query: &str, token_key: &[u8; 32]) -> Result<String, RedactError> {
    let mut statements = DFParser::parse_sql(query).map_err(|_| RedactError::Parse)?;

    let mut redactor = LiteralRedactor {
        key: token_key,
        structural: Vec::new(),
    };
    let mut rendered = Vec::with_capacity(statements.len());
    for statement in statements.iter_mut() {
        let DFStatement::Statement(inner) = statement else {
            return Err(RedactError::Unsupported {
                kind: unsupported_kind(statement),
            });
        };
        let stmt: &mut Statement = inner;
        let _ = VisitMut::visit(stmt, &mut redactor);
        rendered.push(statement.to_string());
    }
    Ok(rendered.join("; "))
}

/// A fixed, input-independent label naming a non-`Statement` `DFParser`
/// extension, for [`RedactError::Unsupported`]. Never derived from input text.
fn unsupported_kind(statement: &DFStatement) -> &'static str {
    match statement {
        // Unreachable: the caller only reaches here for non-`Statement`
        // variants, but keep the arm so the match stays exhaustive.
        DFStatement::Statement(_) => "STATEMENT",
        DFStatement::CreateExternalTable(_) => "CREATE EXTERNAL TABLE",
        DFStatement::CopyTo(_) => "COPY",
        DFStatement::Explain(_) => "EXPLAIN",
        DFStatement::Reset(_) => "RESET",
    }
}

/// Visits every `Value` node in a statement and replaces each identifying
/// literal with a keyed token, in place, while leaving structural numeric
/// literals (see module docs) readable.
struct LiteralRedactor<'a> {
    key: &'a [u8; 32],
    /// A stack of per-`Query` snapshots of the structural value positions,
    /// captured on entry and restored on exit. See [`Self::pre_visit_query`].
    /// Each position is `Some(original)` only when it is a structural literal
    /// worth restoring (a bare `Value::Number`); `None` marks a position that
    /// is still visited (so the snapshot and restore passes see the identical
    /// position sequence -- required for `for_each_structural_value` to stay
    /// aligned) but must NOT be restored, because it was never a number and
    /// `pre_visit_value` genuinely tokenized it.
    structural: Vec<Vec<Option<Value>>>,
}

impl VisitorMut for LiteralRedactor<'_> {
    type Break = ();

    /// Snapshot this query's structural value literals before the walk
    /// descends into it.
    ///
    /// sqlparser's `VisitorMut` hooks are too coarse to skip specific fields:
    /// `pre_visit_query` fires once for the whole `Query`, with no per-field
    /// hook that would let a flag be toggled around only the `LIMIT`/`OFFSET`/
    /// `FETCH`/`ORDER BY` tail. So instead of preventing tokenization there, we
    /// snapshot those positions here (before `pre_visit_value` tokenizes them)
    /// and restore the originals in `post_visit_query`, after the walk is done.
    /// The stack keeps nested subqueries independent.
    ///
    /// Only a bare `Value::Number` is captured as `Some`: those are the
    /// genuinely structural positions (a `LIMIT`/`OFFSET`/`FETCH` count or an
    /// `ORDER BY` ordinal) this carve-out exists for. A non-number literal in
    /// one of these positions (`LIMIT 'x'`, `ORDER BY 'x'` -- not valid SQL
    /// shape-wise for most of these but sqlparser still parses a bare string
    /// there) is captured as `None` and left to `pre_visit_value`'s
    /// tokenization, which `post_visit_query` below must not undo.
    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let mut snapshot = Vec::new();
        for_each_structural_value(query, |vws| {
            snapshot.push(match &vws.value {
                Value::Number(..) => Some(vws.value.clone()),
                _ => None,
            });
        });
        self.structural.push(snapshot);
        ControlFlow::Continue(())
    }

    /// Restore the structural value literals tokenized during the walk to
    /// their pre-visit form, but only at a position the snapshot captured as
    /// `Some` (a bare number). A `None` position was never a number and must
    /// keep whatever `pre_visit_value` tokenized it to -- restoring it would
    /// silently undo the redaction and leak the literal in a structural
    /// position that isn't actually structural. Positions align with the
    /// snapshot because tokenization only rewrites `Value` contents, never
    /// the query's structure.
    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        if let Some(snapshot) = self.structural.pop() {
            let mut originals = snapshot.into_iter();
            for_each_structural_value(query, |vws| {
                if let Some(Some(original)) = originals.next() {
                    vws.value = original;
                }
            });
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<Self::Break> {
        if let Some(text) = redactable_content(&value.value) {
            value.value = Value::SingleQuotedString(audit_token(self.key, text.as_bytes()));
        }
        ControlFlow::Continue(())
    }
}

/// Invoke `f` on every `ValueWithSpan` that sits in a *structural* position of
/// `query`: a `LIMIT`/`OFFSET`/`FETCH` count, or a bare positional `ORDER BY`
/// ordinal. These shape the query rather than naming a subject, so they stay
/// readable (see module docs). Only bare `Expr::Value` nodes are structural: a
/// non-literal expression in one of these positions (a subquery, an
/// arithmetic expression, a `CASE`) is left to tokenize normally, so a value
/// hidden inside it never leaks.
///
/// The traversal order is deterministic and depends only on `query`'s
/// structure, so a capture pass and a restore pass visit the same positions in
/// the same order.
fn for_each_structural_value<F: FnMut(&mut ValueWithSpan)>(query: &mut Query, mut f: F) {
    if let Some(order_by) = &mut query.order_by
        && let OrderByKind::Expressions(exprs) = &mut order_by.kind
    {
        for item in exprs {
            if let Expr::Value(vws) = &mut item.expr {
                f(vws);
            }
        }
    }
    match &mut query.limit_clause {
        Some(LimitClause::LimitOffset { limit, offset, .. }) => {
            if let Some(Expr::Value(vws)) = limit.as_mut() {
                f(vws);
            }
            if let Some(offset) = offset.as_mut()
                && let Expr::Value(vws) = &mut offset.value
            {
                f(vws);
            }
        }
        Some(LimitClause::OffsetCommaLimit { offset, limit }) => {
            if let Expr::Value(vws) = offset {
                f(vws);
            }
            if let Expr::Value(vws) = limit {
                f(vws);
            }
        }
        None => {}
    }
    if let Some(fetch) = &mut query.fetch
        && let Some(Expr::Value(vws)) = fetch.quantity.as_mut()
    {
        f(vws);
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
        assert!(matches!(err, RedactError::Parse));
    }

    // --- F1: unsupported statement shapes fail loudly, never pass through ---

    #[test]
    fn explain_is_rejected_not_passed_through_unredacted() {
        // `EXPLAIN` parses to `DFStatement::Explain`, whose Display re-renders
        // the inner statement -- including its `WHERE` literal -- verbatim. The
        // redactor must fail rather than echo the unredacted 'alice'.
        let err = redact("EXPLAIN SELECT * FROM t WHERE a = 'alice'", &KEY_A)
            .expect_err("EXPLAIN must be rejected");
        assert_eq!(err, RedactError::Unsupported { kind: "EXPLAIN" });
    }

    #[test]
    fn copy_to_is_rejected_not_passed_through_unredacted() {
        let err = redact(
            "COPY (SELECT a FROM t WHERE a = 'alice') TO 'out.parquet'",
            &KEY_A,
        )
        .expect_err("COPY must be rejected");
        assert_eq!(err, RedactError::Unsupported { kind: "COPY" });
    }

    #[test]
    fn create_external_table_is_rejected() {
        let err = redact(
            "CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION '/data/secret'",
            &KEY_A,
        )
        .expect_err("CREATE EXTERNAL TABLE must be rejected");
        assert_eq!(
            err,
            RedactError::Unsupported {
                kind: "CREATE EXTERNAL TABLE"
            }
        );
    }

    #[test]
    fn multi_statement_with_one_unsupported_fails_the_whole_call() {
        // A normal SELECT followed by an EXPLAIN must not partially redact and
        // return the SELECT's redacted form: the whole call fails.
        let err = redact(
            "SELECT * FROM t WHERE a = 'alice'; EXPLAIN SELECT * FROM t WHERE b = 'bob'",
            &KEY_A,
        )
        .expect_err("a batch containing an unsupported statement fails whole");
        assert_eq!(err, RedactError::Unsupported { kind: "EXPLAIN" });
    }

    // --- F2: structural numeric literals stay readable, value literals do not ---

    #[test]
    fn limit_offset_and_positional_order_by_stay_readable() {
        // `ORDER BY 1`, `LIMIT 10`, `OFFSET 5` are structural and survive
        // verbatim, while a `WHERE` literal in the same query still tokenizes.
        let out = redact(
            "SELECT * FROM t WHERE x = 'secret' ORDER BY 1 LIMIT 10 OFFSET 5",
            &KEY_A,
        )
        .expect("redacts");

        assert!(out.contains("ORDER BY 1"), "positional ordinal lost: {out}");
        assert!(out.contains("LIMIT 10"), "LIMIT count lost: {out}");
        assert!(out.contains("OFFSET 5"), "OFFSET count lost: {out}");

        // The WHERE value is still redacted.
        assert!(!out.contains("secret"), "WHERE literal leaked: {out}");
        assert!(out.contains("tok_"), "WHERE value should tokenize: {out}");

        reparse(&out);
    }

    #[test]
    fn fetch_count_stays_readable() {
        let out = redact("SELECT * FROM t FETCH FIRST 10 ROWS ONLY", &KEY_A).expect("redacts");
        assert!(out.contains("10"), "FETCH count lost: {out}");
        assert!(
            !out.contains("tok_"),
            "no token expected for a bare FETCH: {out}"
        );
        reparse(&out);
    }

    #[test]
    fn non_ordinal_order_by_expression_still_redacts_hidden_values() {
        // The carve-out is only for a *bare* positional ordinal. A value buried
        // inside an ORDER BY expression is not structural and must tokenize, or
        // the F1 no-passthrough guarantee would have a hole here.
        let out = redact(
            "SELECT a FROM t ORDER BY CASE WHEN a = 'alice' THEN 1 ELSE 2 END",
            &KEY_A,
        )
        .expect("redacts");
        assert!(
            !out.contains("alice"),
            "value in ORDER BY expr leaked: {out}"
        );
        assert!(out.contains("tok_"), "hidden value should tokenize: {out}");
        reparse(&out);
    }

    #[test]
    fn non_numeric_structural_positions_still_tokenize() {
        // The structural carve-out is for a bare NUMBER only. A string
        // literal sitting in one of the four structural positions (ORDER BY,
        // LIMIT, OFFSET, FETCH) is not a real ordinal or count and must still
        // tokenize -- the snapshot/restore mechanism must not blindly restore
        // whatever `pre_visit_value` tokenized there.
        let cases = [
            (
                "SELECT a FROM t ORDER BY 'alice@example.com'",
                "alice@example.com",
            ),
            (
                "SELECT a FROM t ORDER BY b, 'secret-user-id'",
                "secret-user-id",
            ),
            ("SELECT a FROM t LIMIT 'topsecret'", "topsecret"),
            ("SELECT a FROM t OFFSET 'topsecret'", "topsecret"),
            (
                "SELECT a FROM t FETCH FIRST 'topsecret' ROWS ONLY",
                "topsecret",
            ),
        ];
        for (query, literal) in cases {
            let out = redact(query, &KEY_A).expect("redacts");
            assert!(
                !out.contains(literal),
                "non-numeric structural literal leaked for {query:?}: {out}"
            );
            assert!(
                out.contains("tok_"),
                "non-numeric structural literal should tokenize for {query:?}: {out}"
            );
            reparse(&out);
        }
    }

    #[test]
    fn substr_numeric_arguments_are_tokenized() {
        // Decision: `substr`'s positional numeric arguments (1, 3) are NOT
        // treated as structural. The structural carve-out is deliberately
        // narrow -- only LIMIT/OFFSET/FETCH counts and positional ORDER BY --
        // and a number passed to an arbitrary function can just as easily be an
        // id as an offset. Tokenizing is the safe default; a false negative
        // here would be a PII leak, a false positive only loses a length.
        let out =
            redact("SELECT substr(a, 1, 3) FROM t WHERE b = 'alice'", &KEY_A).expect("redacts");
        // sqlparser re-renders function names upper-cased (`SUBSTR`).
        assert!(
            out.to_uppercase().contains("SUBSTR("),
            "function name preserved: {out}"
        );
        assert!(!out.contains("alice"), "WHERE literal leaked: {out}");
        // The exact tokens for the numeric arguments prove they were redacted.
        assert!(
            out.contains(&audit_token(&KEY_A, b"1")),
            "substr arg 1 not tokenized: {out}"
        );
        assert!(
            out.contains(&audit_token(&KEY_A, b"3")),
            "substr arg 3 not tokenized: {out}"
        );
        reparse(&out);
    }

    // --- F3: RedactError never carries input-derived text ---

    #[test]
    fn error_display_never_leaks_input_literals() {
        // The raw parser message DOES quote the offending literal verbatim:
        // this documents the leak F3 closes. A trailing unexpected string
        // literal makes sqlparser echo it (`... found: 'topsecret' ...`).
        let probe = "SELECT * FROM t WHERE a = 'ok' 'topsecret'";
        let raw = DFParser::parse_sql(probe)
            .expect_err("malformed input")
            .to_string();
        assert!(
            raw.contains("topsecret"),
            "raw parser message unexpectedly safe, F3 rationale stale: {raw}"
        );

        // Our Parse error carries none of it.
        let parse_err = redact(probe, &KEY_A).expect_err("rejected");
        assert!(matches!(parse_err, RedactError::Parse));
        assert!(
            !parse_err.to_string().contains("topsecret"),
            "Parse Display leaked a literal: {parse_err}"
        );

        // The Unsupported variant's Display is a fixed label too.
        let unsup_err =
            redact("EXPLAIN SELECT * FROM t WHERE a = 'alice'", &KEY_A).expect_err("rejected");
        assert!(
            !unsup_err.to_string().contains("alice"),
            "Unsupported Display leaked a literal: {unsup_err}"
        );
    }
}
