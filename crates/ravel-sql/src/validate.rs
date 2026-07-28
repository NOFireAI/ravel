//! Security invariant 1: read-only single-statement SQL
//! (docs/arrow-datafusion-plan.md section 2 "Security invariants", review
//! F16).
//!
//! [`validate`] runs on the raw request text *before* any planning, catalog
//! resolution, or `SessionContext` construction. It accepts exactly one
//! statement and only when that statement is a read-only `Statement::Query`.
//! Everything else -- DDL in every form (including DataFusion's own
//! `CREATE EXTERNAL TABLE` extension), DML, `COPY`, `SET`/`RESET`,
//! transaction control, `EXPLAIN` (both the ANSI and the DataFusion
//! extension form), and a multi-statement body -- is rejected with a typed
//! error the endpoint maps to HTTP 400.
//!
//! Parsing here uses `datafusion::sql::parser::DFParser`, not bare
//! sqlparser: DFParser is the same front end `SessionContext::sql` uses, so
//! a statement that parses into a DataFusion extension variant
//! (`CreateExternalTable`, `CopyTo`, `Explain`, `Reset`) is seen here in the
//! same shape the planner would see it. A gate built on bare sqlparser would
//! either fail to parse those or classify them differently from the planner,
//! which is exactly the kind of gap the invariant exists to close.
//!
//! A `Query` is not automatically read-only in sqlparser's grammar: its body
//! is a `SetExpr`, which has `Insert`/`Update`/`Delete`/`Merge` variants
//! (`WITH ... INSERT ...` parses as `Statement::Query`). [`validate`]
//! therefore walks the whole query tree -- body, set operations, CTEs, and
//! parenthesized subqueries -- and rejects any statement-bearing node.
//!
//! Subset validation (the aggregate allowlist, grouped `min`/`max`) rides
//! along here rather than in a second pass.
//!
//! The v1 SQL subset admits exactly six aggregates: `count`, `sum`, `min`,
//! `max`, `avg`, `mean` ([`ADMITTED_AGGREGATES`](crate::session::ADMITTED_AGGREGATES)).
//! Every other aggregate DataFusion registers by default is excluded, because
//! none of them meets the exactness admission rule (ADR-0022 decision 1):
//! `avg`'s built-in lane-parallel batch sum has no portable sequential
//! reference, so it is admitted only after crate::session replaces it with a
//! sequential-fold UDAF (crate::avg, ADR-0022 decisions 3, 4); the
//! stddev/variance/covariance/correlation family folds a floating mean
//! (Welford, or a grouped sum-of-products state with a lossy merge) that no
//! naive reference reproduces bit-for-bit, and the remaining defaults
//! (`median`, the `regr_*` and `approx_*` families, `string_agg`, `array_agg`,
//! `first_value`/`last_value`/`nth_value`, the bit and bool aggregates,
//! `grouping`, `percentile_cont`) are unverified by the differential gate.
//! ADR-0022 decision 2 makes exclusion the default: this walk rejects any call
//! spelled as one of the excluded names ([`EXCLUDED_AGGREGATES`]) with an error
//! naming the admitted set, and `crate::session::build_session` enforces the
//! same allowlist at registration by deregistering every default UDAF outside
//! the admitted set. The walk exists for the good error message; the
//! deregistration is the backstop. A CI test
//! (`crate::session`'s `admitted_and_excluded_aggregates_cover_the_default_\
//! registrations`) asserts the excluded list plus the admitted set exactly
//! cover the default registrations, so a DataFusion upgrade that adds a default
//! aggregate fails closed instead of silently widening the surface.
//!
//! `avg`/`mean` are admitted (ADR-0022 decisions 3, 4, issue #172): they are
//! in the allowlist, not this reject list, and crate::session registers a
//! custom sequential-fold UDAF (crate::avg) in place of the built-in whose
//! lane-parallel batch sum was unpinnable.
//!
//! `min`/`max` are fully in the v1 subset, grouped and ungrouped alike.
//! DataFusion's grouped accumulator does not use a total order (it folds
//! `partial_cmp` from an `f64::MAX`/`f64::MIN` seed and disagrees with the
//! ungrouped path on NaN, signed zero, and all-infinite groups), so
//! crate::session registers a total-order MIN/MAX UDAF over the built-in that
//! owns float extreme semantics for both paths (ADR-0023). The interim
//! validation-time rejection of grouped `min`/`max` was removed when that
//! UDAF landed; no walk here guards min/max any more, because the registry
//! replacement is structurally total.

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Query, SetExpr, Statement,
    TableFactor, Visit, Visitor,
};
use std::ops::ControlFlow;

/// Every aggregate UDAF name (primary spelling and alias) the default
/// DataFusion session registers that is **not** in the admitted set
/// ([`ADMITTED_AGGREGATES`](crate::session::ADMITTED_AGGREGATES): `count`,
/// `sum`, `min`, `max`). ADR-0022 decision 2 makes exclusion the default: the
/// aggregate walk rejects any call spelled as one of these, and
/// `crate::session::build_session` deregisters the same names at registration.
///
/// This list is kept exhaustive by a CI test in `crate::session`
/// (`admitted_and_excluded_aggregates_cover_the_default_registrations`) that
/// asserts these names plus the admitted set exactly cover the UDAF names a
/// default session registers, so a DataFusion version bump that adds a default
/// aggregate breaks that test rather than silently widening the SQL surface.
/// `avg`/`mean` are not here: they are admitted through the allowlist, their
/// built-in accumulator replaced by a custom sequential-fold UDAF (crate::avg,
/// ADR-0022 decisions 3, 4, issue #172).
pub(crate) const EXCLUDED_AGGREGATES: [&str; 39] = [
    "approx_distinct",
    "approx_median",
    "approx_percentile_cont",
    "approx_percentile_cont_with_weight",
    "array_agg",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "corr",
    "covar",
    "covar_pop",
    "covar_samp",
    "first_value",
    "grouping",
    "last_value",
    "median",
    "nth_value",
    "percentile_cont",
    "quantile_cont",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "var",
    "var_pop",
    "var_population",
    "var_samp",
    "var_sample",
];

/// A request rejected by the read-only single-statement gate, before any
/// planning happened.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// The body contained no statement at all.
    #[error("the SQL request contains no statement")]
    Empty,

    /// More than one statement in one request body.
    #[error("only a single SQL statement is accepted; the request contains {count}")]
    MultipleStatements { count: usize },

    /// The body did not parse. The message quotes only the caller's own
    /// input, so it is safe to return verbatim.
    #[error("SQL parse error: {0}")]
    Parse(String),

    /// The statement parsed but is not a read-only query.
    #[error(
        "{kind} is not permitted on the read-only SQL endpoint; \
         only a single SELECT query is accepted"
    )]
    NotReadOnly { kind: &'static str },

    /// The query is a `Statement::Query` but contains a write node
    /// (`WITH ... INSERT`, a `VALUES`-wrapped DML body, and so on).
    #[error(
        "the query contains a write operation ({kind}); \
         the SQL endpoint is read-only"
    )]
    WriteInQuery { kind: &'static str },

    /// An aggregate outside the v1 admitted set (`count`, `sum`, `min`, `max`,
    /// `avg`, `mean`) appeared in the query. Every other aggregate DataFusion
    /// registers by default is excluded (ADR-0022 decision 2); `name` is the
    /// offending lowercased spelling.
    #[error(
        "{name} is not part of the v1 SQL aggregate subset; \
         the admitted aggregates are count, sum, min, max, avg, mean \
         (docs/adrs/0022-floating-aggregate-exactness.md)"
    )]
    ExcludedAggregate { name: String },
}

/// Parse `sql` and accept it only if it is exactly one read-only
/// `Statement::Query` inside the v1 subset. Returns before any planning.
pub fn validate(sql: &str) -> Result<(), ValidationError> {
    let statements = DFParser::parse_sql(sql)
        .map_err(|e| ValidationError::Parse(strip_prefix(&e.to_string())))?;

    if statements.len() > 1 {
        return Err(ValidationError::MultipleStatements {
            count: statements.len(),
        });
    }
    let statement = statements.front().ok_or(ValidationError::Empty)?;

    let query = match statement {
        DFStatement::Statement(inner) => match inner.as_ref() {
            Statement::Query(query) => query.as_ref(),
            other => {
                return Err(ValidationError::NotReadOnly {
                    kind: ansi_statement_kind(other),
                });
            }
        },
        DFStatement::CreateExternalTable(_) => {
            return Err(ValidationError::NotReadOnly {
                kind: "CREATE EXTERNAL TABLE",
            });
        }
        DFStatement::CopyTo(_) => {
            return Err(ValidationError::NotReadOnly { kind: "COPY" });
        }
        DFStatement::Explain(_) => {
            return Err(ValidationError::NotReadOnly { kind: "EXPLAIN" });
        }
        DFStatement::Reset(_) => {
            return Err(ValidationError::NotReadOnly { kind: "RESET" });
        }
    };

    reject_writes_in_query(query)?;
    reject_excluded_aggregates(query)?;
    Ok(())
}

/// sqlparser prefixes its errors with "sql parser error: "; the endpoint
/// adds its own framing, so drop the duplicate.
fn strip_prefix(msg: &str) -> String {
    msg.strip_prefix("SQL error: ")
        .unwrap_or(msg)
        .trim()
        .to_string()
}

/// A stable, allocation-free name for a rejected ANSI statement kind. The
/// arms named explicitly are the ones the plan calls out; everything else
/// collapses to a generic label rather than echoing the statement text back
/// (statement `Display` re-renders the caller's own SQL, which is safe, but
/// a fixed vocabulary keeps the client contract stable and the error body
/// free of anything derived from server state).
fn ansi_statement_kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Merge { .. } => "MERGE",
        Statement::Truncate { .. } => "TRUNCATE",
        Statement::Copy { .. } | Statement::CopyIntoSnowflake { .. } => "COPY",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::CreateSchema { .. } => "CREATE SCHEMA",
        Statement::CreateDatabase { .. } => "CREATE DATABASE",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::CreateFunction(_) => "CREATE FUNCTION",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::Set(_) => "SET",
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. } => "transaction control",
        Statement::Explain { .. } | Statement::ExplainTable { .. } => "EXPLAIN",
        Statement::Prepare { .. } | Statement::Execute { .. } | Statement::Deallocate { .. } => {
            "prepared-statement control"
        }
        Statement::Grant { .. } | Statement::Revoke { .. } => "access control",
        Statement::Call(_) => "CALL",
        Statement::Use(_) => "USE",
        Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowFunctions { .. }
        | Statement::ShowCreate { .. } => "SHOW",
        _ => "this statement kind",
    }
}

/// Walk the query tree for statement-bearing `SetExpr` nodes. `WITH x AS
/// (...) INSERT ...` and friends parse as `Statement::Query`, so accepting
/// every `Statement::Query` unchecked would let DML through the gate.
fn reject_writes_in_query(query: &Query) -> Result<(), ValidationError> {
    let mut found: Option<&'static str> = None;
    let flow = query.visit(&mut WriteFinder { found: &mut found });
    if flow.is_break()
        && let Some(kind) = found
    {
        return Err(ValidationError::WriteInQuery { kind });
    }
    Ok(())
}

struct WriteFinder<'a> {
    found: &'a mut Option<&'static str>,
}

impl Visitor for WriteFinder<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(kind) = write_kind(&query.body) {
            *self.found = Some(kind);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    /// `SELECT ... FROM (INSERT ...)`-shaped derived tables and any other
    /// table factor whose body is a query are covered by `pre_visit_query`
    /// (the visitor descends into them), but a derived table holding a
    /// function-style table factor is not a query at all, so nothing else
    /// is needed here. This override exists only to make the traversal's
    /// coverage explicit rather than implied.
    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Derived { subquery, .. } = factor
            && let Some(kind) = write_kind(&subquery.body)
        {
            *self.found = Some(kind);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn write_kind(body: &SetExpr) -> Option<&'static str> {
    match body {
        SetExpr::Insert(_) => Some("INSERT"),
        SetExpr::Update(_) => Some("UPDATE"),
        SetExpr::Delete(_) => Some("DELETE"),
        SetExpr::Merge(_) => Some("MERGE"),
        SetExpr::Select(_)
        | SetExpr::Query(_)
        | SetExpr::SetOperation { .. }
        | SetExpr::Values(_)
        | SetExpr::Table(_) => None,
    }
}

/// Whether `bare` names an aggregate excluded from the v1 subset, i.e. any
/// default-registered aggregate outside the admitted set (see
/// [`EXCLUDED_AGGREGATES`]).
fn is_excluded_aggregate(bare: &str) -> bool {
    EXCLUDED_AGGREGATES.contains(&bare)
}

/// Reject any excluded aggregate anywhere in the query, including inside
/// subqueries and nested function arguments. ADR-0022 decision 2 flips the
/// subset from a per-function blacklist to an allowlist: only `count`, `sum`,
/// `min`, `max`, `avg`, `mean` are admitted, so this walk rejects every other
/// default aggregate. `crate::session::build_session` deregisters the same
/// names as a backstop; the walk exists for the error message that names the
/// admitted set.
fn reject_excluded_aggregates(query: &Query) -> Result<(), ValidationError> {
    if let ControlFlow::Break(name) = query.visit(&mut ExcludedAggregateFinder) {
        return Err(ValidationError::ExcludedAggregate { name });
    }
    Ok(())
}

struct ExcludedAggregateFinder;

impl Visitor for ExcludedAggregateFinder {
    /// The offending lowercased bare name, carried out so the error can name
    /// it.
    type Break = String;

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<String> {
        if let SqlExpr::Function(func) = expr {
            let name = func.name.to_string().to_ascii_lowercase();
            // Match the bare name and any schema-qualified spelling
            // (`public.avg`), which the planner resolves to the same UDAF.
            let bare = name.rsplit('.').next().unwrap_or(name.as_str());
            if is_excluded_aggregate(bare) {
                return ControlFlow::Break(bare.to_string());
            }
            // sqlparser does not descend into `FunctionArguments::List`
            // expressions from `pre_visit_expr` on the enclosing call in
            // every version; walk them explicitly so `sum(avg(x))`-shaped
            // nesting cannot hide an excluded aggregate.
            if let FunctionArguments::List(list) = &func.args {
                for arg in &list.args {
                    let inner = match arg {
                        FunctionArg::Named { arg, .. }
                        | FunctionArg::ExprNamed { arg, .. }
                        | FunctionArg::Unnamed(arg) => arg,
                    };
                    if let FunctionArgExpr::Expr(inner) = inner
                        && let ControlFlow::Break(found) = inner.visit(self)
                    {
                        return ControlFlow::Break(found);
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn reject(sql: &str) -> ValidationError {
        validate(sql).expect_err("must be rejected")
    }

    #[test]
    fn plain_select_is_accepted() {
        validate("SELECT ts, value FROM samples WHERE ts > 0 ORDER BY ts LIMIT 10")
            .expect("read-only select");
    }

    #[test]
    fn aggregates_in_the_v1_subset_are_accepted() {
        validate(
            "SELECT series_id, count(value), sum(value) \
             FROM samples GROUP BY series_id ORDER BY series_id",
        )
        .expect("v1 aggregate subset");
    }

    /// `min`/`max` are in the v1 subset in every shape now that the
    /// total-order UDAF (ADR-0023, crate::minmax) owns float extreme
    /// semantics; nothing in this walk rejects them. Grouped correctness is
    /// gated by the differential and golden cases in tests/differential.rs,
    /// not here (validate is a pure text function with no session).
    #[test]
    fn min_max_are_accepted_grouped_and_ungrouped() {
        for sql in [
            "SELECT min(value), max(value) FROM samples",
            "SELECT series_id, min(value) FROM samples GROUP BY series_id",
            "SELECT series_id, MAX(value) FROM samples GROUP BY series_id",
            "SELECT series_id FROM samples GROUP BY series_id HAVING max(value) > 1",
            "SELECT series_id, min(value) FROM samples GROUP BY ALL",
            "SELECT series_id FROM samples GROUP BY series_id ORDER BY max(value)",
            "SELECT series_id FROM samples GROUP BY series_id ORDER BY min(value) + 1",
        ] {
            validate(sql).unwrap_or_else(|e| panic!("min/max must be accepted: {sql}: {e}"));
        }
    }

    #[test]
    fn create_external_table_is_rejected() {
        assert_eq!(
            reject("CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION '/tmp/x'"),
            ValidationError::NotReadOnly {
                kind: "CREATE EXTERNAL TABLE"
            }
        );
    }

    #[test]
    fn copy_to_is_rejected() {
        assert_eq!(
            reject("COPY (SELECT 1) TO 's3://evil/out.parquet'"),
            ValidationError::NotReadOnly { kind: "COPY" }
        );
    }

    #[test]
    fn insert_is_rejected() {
        assert_eq!(
            reject("INSERT INTO samples VALUES (1, 2.0)"),
            ValidationError::NotReadOnly { kind: "INSERT" }
        );
    }

    #[test]
    fn set_is_rejected() {
        assert_eq!(
            reject("SET datafusion.execution.batch_size = 1"),
            ValidationError::NotReadOnly { kind: "SET" }
        );
    }

    #[test]
    fn multi_statement_is_rejected_by_count_not_by_kind() {
        assert_eq!(
            reject("SELECT 1; SELECT 2"),
            ValidationError::MultipleStatements { count: 2 }
        );
    }

    #[test]
    fn multi_statement_hiding_dml_after_a_select_is_rejected() {
        assert_eq!(
            reject("SELECT 1; INSERT INTO samples VALUES (1, 2.0)"),
            ValidationError::MultipleStatements { count: 2 }
        );
    }

    #[test]
    fn explain_and_explain_analyze_are_rejected() {
        assert_eq!(
            reject("EXPLAIN SELECT 1"),
            ValidationError::NotReadOnly { kind: "EXPLAIN" }
        );
        assert_eq!(
            reject("EXPLAIN ANALYZE SELECT 1"),
            ValidationError::NotReadOnly { kind: "EXPLAIN" }
        );
    }

    #[test]
    fn ddl_and_dml_families_are_rejected() {
        for (sql, kind) in [
            ("CREATE TABLE t (a INT)", "CREATE TABLE"),
            ("CREATE VIEW v AS SELECT 1", "CREATE VIEW"),
            ("CREATE SCHEMA s", "CREATE SCHEMA"),
            ("DROP TABLE samples", "DROP"),
            ("UPDATE samples SET value = 1", "UPDATE"),
            ("DELETE FROM samples", "DELETE"),
            ("BEGIN TRANSACTION", "transaction control"),
            ("COMMIT", "transaction control"),
            ("PREPARE p AS SELECT 1", "prepared-statement control"),
        ] {
            assert_eq!(
                reject(sql),
                ValidationError::NotReadOnly { kind },
                "sql: {sql}"
            );
        }
    }

    /// `WITH ... INSERT` parses as `Statement::Query`, so a gate that only
    /// checked the outer variant would pass it straight to the planner.
    #[test]
    fn write_hidden_inside_a_query_body_is_rejected() {
        let err = validate("WITH c AS (SELECT 1) INSERT INTO samples VALUES (1, 2.0)");
        match err {
            // Either shape is a correct rejection: some dialect versions
            // parse this as a top-level INSERT, others as a Query whose body
            // is SetExpr::Insert. Both must be refused.
            Err(ValidationError::WriteInQuery { kind: "INSERT" })
            | Err(ValidationError::NotReadOnly { kind: "INSERT" }) => {}
            other => panic!("WITH ... INSERT must be rejected, got {other:?}"),
        }
    }

    /// `avg`/`mean` are admitted (ADR-0022 decisions 3, 4, issue #172): the
    /// custom sequential-fold UDAF (crate::avg) replaces the built-in, so the
    /// validation walk no longer rejects them, grouped or ungrouped, and in
    /// every case spelling. Correctness of the bits is gated in
    /// tests/differential.rs; this test only asserts the text gate passes.
    #[test]
    fn avg_and_mean_are_accepted_grouped_and_ungrouped() {
        for sql in [
            "SELECT avg(value) FROM samples",
            "SELECT mean(value) FROM samples",
            "SELECT AVG(value) FROM samples",
            "SELECT s FROM (SELECT AVG(value) AS s FROM samples)",
            "SELECT max(value) FROM samples HAVING Avg(value) > 1",
            "SELECT series_id, avg(value) FROM samples GROUP BY series_id",
            "SELECT series_id, mean(value) FROM samples GROUP BY series_id",
            "SELECT series_id FROM samples GROUP BY series_id ORDER BY avg(value)",
        ] {
            validate(sql).unwrap_or_else(|e| panic!("avg/mean must be accepted: {sql}: {e}"));
        }
    }

    /// The error message names the full admitted set, so a client sees every
    /// aggregate it can use, not just the ones the older ticket admitted.
    #[test]
    fn excluded_aggregate_message_names_the_admitted_set() {
        let err = reject("SELECT median(value) FROM samples");
        let msg = err.to_string();
        for admitted in ["count", "sum", "min", "max", "avg", "mean"] {
            assert!(
                msg.contains(admitted),
                "message must name the admitted aggregate {admitted}: {msg}"
            );
        }
    }

    #[test]
    fn stddev_and_variance_family_is_rejected() {
        for func in [
            "stddev",
            "stddev_samp",
            "stddev_pop",
            "var",
            "var_samp",
            "var_pop",
        ] {
            assert_eq!(
                reject(&format!("SELECT {func}(value) FROM samples")),
                ValidationError::ExcludedAggregate {
                    name: func.to_string()
                },
                "ungrouped {func} must be rejected"
            );
            assert_eq!(
                reject(&format!(
                    "SELECT series_id, {func}(value) FROM samples GROUP BY series_id"
                )),
                ValidationError::ExcludedAggregate {
                    name: func.to_string()
                },
                "grouped {func} must be rejected"
            );
        }
    }

    #[test]
    fn covariance_and_correlation_are_rejected() {
        for func in ["covar_samp", "covar_pop", "corr"] {
            assert_eq!(
                reject(&format!("SELECT {func}(value, value) FROM samples")),
                ValidationError::ExcludedAggregate {
                    name: func.to_string()
                },
                "{func} must be rejected"
            );
        }
    }

    /// A representative spread across the rest of the default aggregate set
    /// (regression, approx, median, string/array aggregation, bit and bool
    /// families, and the value pickers) is excluded, not just the floating-mean
    /// functions: the allowlist admits only `count`/`sum`/`min`/`max`/`avg`/`mean`.
    #[test]
    fn other_default_aggregates_are_excluded_by_the_allowlist() {
        for func in [
            "median",
            "regr_slope",
            "approx_distinct",
            "string_agg",
            "array_agg",
            "bit_and",
            "bool_or",
            "first_value",
        ] {
            assert_eq!(
                reject(&format!("SELECT {func}(value) FROM samples")),
                ValidationError::ExcludedAggregate {
                    name: func.to_string()
                },
                "{func} must be excluded from the v1 subset"
            );
        }
    }

    #[test]
    fn stddev_var_is_rejected_in_a_subquery_and_case_insensitively() {
        assert_eq!(
            reject("SELECT s FROM (SELECT STDDEV(value) AS s FROM samples)"),
            ValidationError::ExcludedAggregate {
                name: "stddev".to_string()
            }
        );
        assert_eq!(
            reject("SELECT sum(value) FROM samples HAVING Var_Pop(value) > 1"),
            ValidationError::ExcludedAggregate {
                name: "var_pop".to_string()
            }
        );
        // Nested inside another call, as `sum(stddev(x))`.
        assert_eq!(
            reject("SELECT sum(stddev(value)) FROM samples"),
            ValidationError::ExcludedAggregate {
                name: "stddev".to_string()
            }
        );
    }

    #[test]
    fn empty_and_unparsable_bodies_are_rejected_without_planning() {
        assert_eq!(validate(""), Err(ValidationError::Empty));
        assert!(matches!(
            validate("SELECT ((( FROM samples"),
            Err(ValidationError::Parse(_))
        ));
    }
}
