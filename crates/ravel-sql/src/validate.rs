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
//! The v1 SQL subset admits exactly four aggregates: `count`, `sum`, `min`,
//! `max` ([`ADMITTED_AGGREGATES`](crate::session::ADMITTED_AGGREGATES)). Every
//! other aggregate DataFusion registers by default is excluded, because none
//! of them meets the exactness admission rule (ADR-0022 decision 1): `avg`'s
//! lane-parallel batch sum has no portable sequential reference, the
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
//! `avg`/`mean` are excluded here in this ticket (ADR-0022 decision 7's first
//! half, issue #160); a later ticket (issue #172) admits them via a custom
//! sequential UDAF and moves them into the allowlist.
//!
//! `min`/`max` are excluded from the v1 subset **only when grouped**
//! (discovered during the B3 checkpoint review, not in the original plan):
//! DataFusion's ungrouped min/max accumulator compares with `total_cmp`, but
//! the grouped accumulator folds `partial_cmp` from an `f64::MAX`/`f64::MIN`
//! seed. That means grouped min/max disagrees with ungrouped min/max on NaN
//! and signed zero, and for a group whose only value is +/-Inf it returns
//! the unseeded `f64::MAX`/`f64::MIN` rather than the actual value -- a
//! silently wrong answer, not an approximation ("Exact semantics by
//! default"). A differential gate cannot catch this because an independent
//! reference for a grouped accumulator would just be a second copy of the
//! same seed-and-fold algorithm. Ungrouped `min`/`max` are unaffected and
//! stay in the v1 subset.

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query, Select,
    SetExpr, Statement, TableFactor, Visit, Visitor,
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
/// `avg`/`mean` are here in this ticket and move to the allowlist when their
/// custom UDAF lands (ADR-0022 decision 7, issue #172).
pub(crate) const EXCLUDED_AGGREGATES: [&str; 41] = [
    "approx_distinct",
    "approx_median",
    "approx_percentile_cont",
    "approx_percentile_cont_with_weight",
    "array_agg",
    "avg",
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
    "mean",
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

/// The v1 SQL subset rejects `min`/`max` combined with `GROUP BY`.
const GROUPED_MIN_MAX_MESSAGE: &str = "MIN/MAX combined with GROUP BY is not part of the v1 SQL \
     subset (DataFusion's grouped accumulator does not use a total order and can return a \
     wrong extreme for NaN, signed zero, or all-infinite groups); \
     run MIN/MAX without GROUP BY \
     (docs/arrow-datafusion-plan.md section 2 \"Exactness\")";

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

    /// An aggregate outside the v1 admitted set (`count`, `sum`, `min`, `max`)
    /// appeared in the query. Every other aggregate DataFusion registers by
    /// default is excluded (ADR-0022 decision 2); `name` is the offending
    /// lowercased spelling.
    #[error(
        "{name} is not part of the v1 SQL aggregate subset; \
         the admitted aggregates are count, sum, min, max \
         (docs/adrs/0022-floating-aggregate-exactness.md)"
    )]
    ExcludedAggregate { name: String },

    /// `min`/`max` combined with `GROUP BY` is outside the v1 subset.
    #[error("{GROUPED_MIN_MAX_MESSAGE}")]
    GroupedMinMaxUnsupported,
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
    reject_grouped_min_max(query)?;
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
/// `min`, `max` are admitted, so this walk rejects every other default
/// aggregate. `crate::session::build_session` deregisters the same names as a
/// backstop; the walk exists for the error message that names the admitted set.
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

/// Reject `min`/`max` in any select whose own `GROUP BY` is non-trivial.
fn reject_grouped_min_max(query: &Query) -> Result<(), ValidationError> {
    let mut finder = GroupedMinMaxFinder {
        grouped_stack: Vec::new(),
    };
    let flow = query.visit(&mut finder);
    if flow.is_break() {
        return Err(ValidationError::GroupedMinMaxUnsupported);
    }
    Ok(())
}

/// Tracks, per nested select, whether it groups -- a `min`/`max` call is
/// only rejected while the innermost enclosing select is grouped, so a
/// grouped subquery does not reject an ungrouped `min`/`max` in the outer
/// query and vice versa.
///
/// A statement's top-level `ORDER BY` is a field of `Query`, not of `Select`,
/// so the derived visitor reaches it *after* the body `Select` has been
/// visited and popped. A grouped `min`/`max` there binds to the body's
/// grouped hash accumulator exactly as `HAVING` does, so `pre_visit_query`
/// pushes the body `Select`'s grouping for the whole query scope (`ORDER BY`,
/// `LIMIT`, and so on), keeping the stack non-empty while those clauses are
/// visited. `pre_visit_select` still pushes the same grouping again for the
/// body's own projection/`HAVING`; `.last()` sees the same value either way,
/// and both pushes are balanced by their matching post-visit pops.
struct GroupedMinMaxFinder {
    grouped_stack: Vec<bool>,
}

/// Whether a `SELECT`'s own `GROUP BY` routes its `min`/`max` to the grouped
/// accumulator. `GROUP BY ()` (empty expression list) is ungrouped and takes
/// the total-order ungrouped accumulator, so it stays in the v1 subset.
fn select_is_grouped(select: &Select) -> bool {
    match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
    }
}

impl Visitor for GroupedMinMaxFinder {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        // Query-level clauses (`ORDER BY`, `LIMIT`) bind to the body select's
        // grouping. Only a plain `Select` body carries a grouped accumulator;
        // a set operation body pushes `false` (its `ORDER BY` sorts the union
        // output, not a grouped aggregate), and each operand select still
        // pushes its own grouping through `pre_visit_select`.
        let grouped = match query.body.as_ref() {
            SetExpr::Select(select) => select_is_grouped(select),
            _ => false,
        };
        self.grouped_stack.push(grouped);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.grouped_stack.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        self.grouped_stack.push(select_is_grouped(select));
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _select: &Select) -> ControlFlow<()> {
        self.grouped_stack.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<()> {
        if !*self.grouped_stack.last().unwrap_or(&false) {
            return ControlFlow::Continue(());
        }
        if let SqlExpr::Function(func) = expr {
            let name = func.name.to_string().to_ascii_lowercase();
            let bare = name.rsplit('.').next().unwrap_or(name.as_str());
            if bare == "min" || bare == "max" {
                return ControlFlow::Break(());
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

    #[test]
    fn ungrouped_min_max_are_accepted() {
        validate("SELECT min(value), max(value) FROM samples").expect("ungrouped min/max");
    }

    #[test]
    fn grouped_min_max_is_rejected_case_insensitively() {
        assert_eq!(
            reject("SELECT series_id, min(value) FROM samples GROUP BY series_id"),
            ValidationError::GroupedMinMaxUnsupported
        );
        assert_eq!(
            reject("SELECT series_id, MAX(value) FROM samples GROUP BY series_id"),
            ValidationError::GroupedMinMaxUnsupported
        );
        assert_eq!(
            reject("SELECT series_id FROM samples GROUP BY series_id HAVING max(value) > 1"),
            ValidationError::GroupedMinMaxUnsupported
        );
    }

    #[test]
    fn group_by_all_with_min_max_is_rejected() {
        assert_eq!(
            reject("SELECT series_id, min(value) FROM samples GROUP BY ALL"),
            ValidationError::GroupedMinMaxUnsupported
        );
    }

    /// A grouped `min`/`max` inside a subquery is rejected even when the
    /// outer query is not grouped, and an outer grouped `min`/`max` is
    /// rejected even when the inner subquery is not -- the rejection is
    /// scoped to whichever select actually owns the aggregate.
    #[test]
    fn grouped_min_max_scoping_follows_the_owning_select() {
        assert_eq!(
            reject(
                "SELECT s FROM (SELECT series_id, min(value) AS s FROM samples \
                 GROUP BY series_id)"
            ),
            ValidationError::GroupedMinMaxUnsupported
        );
        assert_eq!(
            reject(
                "SELECT series_id, min(value) FROM (SELECT series_id, value FROM samples) \
                 GROUP BY series_id"
            ),
            ValidationError::GroupedMinMaxUnsupported
        );
        // Ungrouped min/max over a grouped subquery's output is fine: the
        // aggregate itself is not grouped.
        validate(
            "SELECT min(s) FROM (SELECT series_id, sum(value) AS s FROM samples \
             GROUP BY series_id)",
        )
        .expect("ungrouped min over a grouped subquery's output");
    }

    /// A query-level `ORDER BY` is a field of `Query`, not of `Select`, so it
    /// is visited after the body select is popped. A grouped `min`/`max` there
    /// binds to the same grouped accumulator as the `HAVING` case and must be
    /// rejected too; an arithmetic expression over the aggregate must not slip
    /// through either.
    #[test]
    fn grouped_min_max_in_query_order_by_is_rejected() {
        assert_eq!(
            reject("SELECT series_id FROM samples GROUP BY series_id ORDER BY max(value)"),
            ValidationError::GroupedMinMaxUnsupported
        );
        assert_eq!(
            reject("SELECT series_id FROM samples GROUP BY series_id ORDER BY min(value) + 1"),
            ValidationError::GroupedMinMaxUnsupported
        );
    }

    /// An ungrouped query keeps `min`/`max` in `ORDER BY`: it uses the
    /// total-order ungrouped accumulator, so the query-scope push of `false`
    /// must leave it accepted.
    #[test]
    fn ungrouped_min_max_in_query_order_by_is_accepted() {
        validate("SELECT max(value) FROM samples ORDER BY max(value)")
            .expect("ungrouped min/max in ORDER BY");
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

    #[test]
    fn avg_is_rejected_and_names_the_admitted_set() {
        let err = reject("SELECT avg(value) FROM samples");
        assert_eq!(
            err,
            ValidationError::ExcludedAggregate {
                name: "avg".to_string()
            }
        );
        let msg = err.to_string();
        for admitted in ["count", "sum", "min", "max"] {
            assert!(
                msg.contains(admitted),
                "message must name the admitted aggregate {admitted}: {msg}"
            );
        }
    }

    #[test]
    fn avg_is_rejected_in_a_subquery_and_case_insensitively() {
        assert_eq!(
            reject("SELECT s FROM (SELECT AVG(value) AS s FROM samples)"),
            ValidationError::ExcludedAggregate {
                name: "avg".to_string()
            }
        );
        assert_eq!(
            reject("SELECT max(value) FROM samples HAVING Avg(value) > 1"),
            ValidationError::ExcludedAggregate {
                name: "avg".to_string()
            }
        );
        // `mean` is avg's registered alias and is excluded under the same name.
        assert_eq!(
            reject("SELECT mean(value) FROM samples"),
            ValidationError::ExcludedAggregate {
                name: "mean".to_string()
            }
        );
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
    /// functions: the allowlist admits only `count`/`sum`/`min`/`max`.
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
