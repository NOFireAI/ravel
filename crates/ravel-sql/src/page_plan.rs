//! The text-to-text page planner behind the paging MCP tools (ADR-1374 D5).
//!
//! [`plan_page`] takes a statement and an optional resume position and returns
//! the statement to execute for that page, the effective `ORDER BY` it will be
//! ordered by, and whether that ordering is a TOTAL order. It executes
//! nothing, resolves nothing, and is not async: the only thing it touches is
//! SQL text.
//!
//! It lives here rather than in `ravel-mcp` because this crate already owns
//! the SQL parser (ADR-1374 D1 keeps SQL parsing behind the engine boundary):
//! a second front end in the MCP crate could classify a statement differently
//! from the one that plans it, which is the gap `crate::validate` exists to
//! close for the security gate and this module closes for paging.
//!
//! # What a total order means here
//!
//! D5 mints a cursor for `ravel_query_sql` "only when the `ORDER BY`, plus a
//! deterministic tiebreak that the tool appends, is a total order over the
//! projection". Total means the effective ordering uniquely determines the row
//! sequence, so a strict keyset predicate cannot skip a row: with two rows
//! equal on every term, the predicate that excludes the page's last row
//! excludes its twin as well.
//!
//! Exactly one table has a row identity to build that on. The `samples` scan
//! emits at most one row per `(series_id, ts)` (`crate::dedup` picks one winner
//! per group under the full dedup total order), so that pair is a key. The four
//! RLOG- and RSPAN-backed tables have none, because ingest is at-least-once and
//! nothing above their scans dedups: the same record can arrive twice and two
//! rows can then tie on every orderable column
//! (`docs/adrs/1374-agent-mcp-server.md` D5 says this of `logs`, and the
//! at-least-once argument carries to `spans`, `alerts`, and `audit`).
//!
//! `alerts` is the one that looks like an exception and is not. Its public
//! schema does carry a non-nullable `(writer_id, writer_epoch, writer_seq)`
//! triple, but that triple is the write identity of the OBJECT a row was read
//! from, stamped by the scan (ADR-1101 decision 1), not of the row: every row
//! decoded from one segment carries the same three values. So it cannot break
//! a tie between two rows of one object, and a retried write puts the second
//! copy in a different object under a different triple. The conclusion is
//! [`NotTotalOrder::NoRowIdentity`] for all four either way; D5's own answer
//! for that case is the equal-group rule the tool applies (fetch `k + 1` rows,
//! drop the whole trailing group of equal tuples), which is the caller's half
//! and not this module's.
//!
//! The identity claim also depends on the statement's shape, not only on its
//! target: a `GROUP BY`, a join, a `DISTINCT`, a CTE, or a set operation
//! projects rows the scan's dedup says nothing about. Those report
//! [`NotTotalOrder::ShapeNotIdentityPreserving`] rather than a tiebreak that
//! would be unsound.
//!
//! # Two things the text has to be read for, not around
//!
//! A pipe operator (`|> ...`) is refused outright
//! ([`PagePlanError::PipeOperator`]). The parser's default dialect accepts
//! them, `Display for Query` re-emits them, and every clause this module reads
//! to classify a statement lives in the `SELECT` body that a pipe runs AFTER:
//! a pipe can impose a row limit, a join, a set operation, a new projection,
//! or an ordering, none of which the body carries. So no pipe is admitted, and
//! the refusal is on the presence of any pipe rather than on a list of pipe
//! kinds, so a pipe kind added by a later sqlparser is refused too instead of
//! being silently admitted.
//!
//! Both that refusal and the row-limit one
//! ([`PagePlanError::RowLimitInStatement`]) are taken over EVERY `Query` in
//! the statement, not the outermost one alone. The wrap re-emits the caller's
//! text verbatim, so a `LIMIT` inside a `WHERE` subquery, a scalar subquery,
//! a derived table, an arm of a set operation, or a CTE is still there on
//! every page, picking a fresh arbitrary set of rows each time it runs. See
//! [`reject_nested_pipes_and_row_limits`] for what that walk covers and for
//! why a nested limit is refused even when its own subquery is deterministic.
//!
//! An `ORDER BY` term that can be NULL is refused as well
//! ([`PagePlanError::OrderTermNullable`]): a keyset comparison against NULL is
//! NULL, so the rows whose term is NULL match no disjunct and appear on no
//! page at all. That is a wrong answer on both the total and the not-total
//! path, because the keyset predicate is rendered whenever a resume is given
//! and is therefore what resumes both. `not_total: Some(..)` rescues nothing
//! here: reporting is not a substitute for refusing.
//!
//! Only a term the text proves NON NULL is admitted, and a term reaches that
//! proof by one of two routes.
//!
//! A bare column reference goes through the target table's public schema, and
//! that lookup holds end to end only when the output name is a bare reference
//! to a column of the `FROM` relation AND that relation is the target base
//! table itself. [`SchemaBasis`] is what settles the second half: a derived
//! table, a CTE, a join, or a `ROLLUP`/`CUBE`/`GROUPING SETS` grouping each
//! leave the schema answering about a column the ordered value did not come
//! from.
//!
//! An alias over an expression is answered by the expression itself
//! ([`expression_is_non_null`]), with no schema involved: a literal and a
//! `count(...)` are NON NULL wherever they are selected from. `SUM`, `MIN`,
//! `MAX` and `AVG` are not, and keep refusing, because each is NULL over
//! empty and over all-NULL input.
//!
//! The refusal says which of two things went wrong.
//! [`PagePlanError::OrderTermNullable`] means the term CAN be NULL: the
//! schema declares that column nullable, and the caller has to order by
//! something else. [`PagePlanError::OrderTermNullabilityUnknown`] means the
//! text does not settle it, and names the missing link, because a caller told
//! that `sum(value)` "is not known to be NON NULL" has no repair to make.
//!
//! # The rewrite
//!
//! The page statement wraps the caller's own statement as a derived table:
//!
//! ```text
//! SELECT * FROM (<statement, its own ORDER BY removed>) AS ravel_page
//!   [WHERE <keyset predicate>]
//!   ORDER BY <effective terms>
//! ```
//!
//! The wrap is what makes the keyset predicate land on the statement's OUTPUT
//! rows. Injected into the caller's own `WHERE`, it would filter before any
//! aggregation, so a page of an aggregate result would drop input rows instead
//! of resuming after the last emitted one. The wrap is also why every
//! effective term has to name a projected output column: a page's rows are all
//! the redeeming caller has to read the next cursor position from.
//!
//! The keyset predicate is the expanded lexicographic form
//! (`(a > v1) OR (a = v1 AND b > v2) OR ...`), with the comparison flipped per
//! `DESC` term, rather than a row-value comparison: the expanded form needs no
//! support for comparing tuples, and each conjunct is a plain binary
//! comparison the providers can push down.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Distinct, Expr as SqlExpr, GroupByExpr, Ident, ObjectName, OrderBy, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, UnaryOperator, Value, Visit, Visitor,
};

use crate::alerts_schema::alerts_schema;
use crate::audit_schema::audit_schema;
use crate::logs_schema::logs_schema;
use crate::schema::public_schema;
use crate::session::{ALERTS_TABLE, AUDIT_TABLE, LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE};
use crate::spans_schema::spans_schema;
use crate::validate::{ValidationError, referenced_base_tables, validate};

/// The alias the page statement gives the caller's own statement as a derived
/// table. Exposed because it is part of the rewritten text: an operator reading
/// a page statement (or a plan for one) sees this name.
pub const PAGE_ALIAS: &str = "ravel_page";

/// The `samples` row identity: post-dedup, at most one row exists per
/// `(series_id, ts)`, so ordering by both uniquely determines the sequence.
/// In `ORDER BY` order, event time first, which is the order every paging
/// caller wants and the one the scan already emits.
const SAMPLES_ROW_IDENTITY: [&str; 2] = ["ts", "series_id"];

/// One term of the effective ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderTerm {
    /// The output column this term orders on, unquoted.
    pub column: String,
    /// `DESC` when true, `ASC` when false. An absent `ASC`/`DESC` in the
    /// statement is `ASC`, matching SQL's own default, and is rendered
    /// explicitly into the page statement so the pinned ordering and the
    /// executed one cannot disagree.
    pub descending: bool,
}

impl OrderTerm {
    /// An ascending term on `column`.
    pub fn ascending(column: impl Into<String>) -> Self {
        OrderTerm {
            column: column.into(),
            descending: false,
        }
    }

    /// A descending term on `column`.
    pub fn descending(column: impl Into<String>) -> Self {
        OrderTerm {
            column: column.into(),
            descending: true,
        }
    }

    /// This term as it is rendered into the page statement's `ORDER BY`.
    pub fn render(&self) -> String {
        let direction = if self.descending { "DESC" } else { "ASC" };
        format!("{} {}", quote_ident(&self.column), direction)
    }
}

/// Why an effective ordering is not a total order. A plan carrying one of
/// these is still executable and still pageable under D5's equal-group rule;
/// what it cannot support is a strict keyset predicate that assumes no ties.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotTotalOrder {
    /// The target table has no row identity: ingest is at-least-once and two
    /// rows can tie on every orderable column, so no appended tiebreak makes
    /// the ordering unique.
    #[error(
        "the {table} table has no row identity, so no appended tiebreak makes \
         an ordering over it unique"
    )]
    NoRowIdentity { table: &'static str },

    /// The statement does not project the target's rows one-for-one, so the
    /// scan's row identity says nothing about the result's.
    #[error(
        "the statement's shape ({shape}) does not preserve the scanned rows \
         one-for-one, so the target's row identity does not carry to the result"
    )]
    ShapeNotIdentityPreserving { shape: &'static str },

    /// The tiebreak columns exist but are not in the projection, so a page's
    /// own rows would not carry the values the next cursor position needs.
    #[error(
        "the tiebreak columns ({}) are not in the projection",
        missing.join(", ")
    )]
    TiebreakNotProjected { missing: Vec<String> },
}

/// A statement that cannot be paged at all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PagePlanError {
    /// The statement is not a single read-only `SELECT`. The gate is
    /// `crate::validate`, the same one the executor runs, so a statement this
    /// accepts is one the executor would accept too.
    #[error("{0}")]
    Invalid(#[from] ValidationError),

    /// The statement names two or more of the five tables. One snapshot per
    /// query admits one signal, so there is no page to plan.
    #[error("a statement naming two signals cannot be paged")]
    CrossSignal,

    /// The statement carries its own `LIMIT`, `OFFSET`, `FETCH`, or `TOP`. The
    /// paging tool owns the row cap; a statement-level one would either be
    /// re-applied per page (returning rows past the bound the caller set) or
    /// silently dropped.
    #[error(
        "a statement carrying its own LIMIT, OFFSET, FETCH or TOP cannot be \
         paged; the page size is the tool's row cap"
    )]
    RowLimitInStatement,

    /// The statement uses a pipe operator (`|> ...`).
    ///
    /// Every classification this module makes reads the `SELECT` body, and a
    /// pipe runs after it: it can impose a row limit, a join, a set operation,
    /// a projection, or an ordering that the body does not carry, while
    /// `Display for Query` re-emits the pipe into the derived table. The
    /// refusal is on the presence of a pipe rather than on its kind, so a kind
    /// a later sqlparser adds is refused rather than admitted unexamined.
    #[error(
        "a statement using a pipe operator cannot be paged; a pipe reshapes \
         the rows after the SELECT body a page is planned from"
    )]
    PipeOperator,

    /// There is nothing to order by: the statement carries no `ORDER BY` and
    /// the target has no row identity to impose one from.
    #[error(
        "no deterministic page exists: the statement carries no ORDER BY and \
         {target} has no row identity to impose one"
    )]
    NoOrdering { target: &'static str },

    /// `ORDER BY ALL`: the term list is not known from the text alone.
    ///
    /// Not reachable through today's front end: `DFParser`'s dialect parses
    /// `ORDER BY all` as a column reference named `all`, so the AST variant
    /// this maps is never produced. It stays a typed refusal rather than an
    /// `unreachable!()` because the alternative to a refusal here is planning
    /// a page over an ordering whose terms were never read.
    #[error("ORDER BY ALL cannot be paged")]
    OrderByAll,

    /// An `ORDER BY` term that is not a plain column reference. A keyset
    /// predicate has to name the term on the left of a comparison, and an
    /// expression term would have to be re-derived rather than read off a row.
    #[error("the ORDER BY term `{term}` is not a column reference")]
    OrderTermNotColumn { term: String },

    /// An `ORDER BY` term that names a column the statement does not project.
    #[error(
        "the ORDER BY term `{column}` is not in the projection, so a page's \
         own rows do not carry the next cursor position"
    )]
    OrderTermNotProjected { column: String },

    /// `NULLS FIRST`/`NULLS LAST` or a `WITH FILL` modifier on a term. A
    /// keyset predicate over a NULL is NULL, so neither can be reproduced by
    /// the rewrite.
    #[error("the ORDER BY option `{option}` on `{column}` cannot be paged")]
    UnsupportedOrderOption {
        column: String,
        option: &'static str,
    },

    /// An `ORDER BY` term the text does not prove NON NULL.
    ///
    /// Refusing the `NULLS FIRST`/`NULLS LAST` spellings is not enough:
    /// omitting the option does not remove NULLs from the sequence, it leaves
    /// their position to a session default. A keyset comparison against a NULL
    /// is NULL, so every row whose term is NULL matches no disjunct and lands
    /// on no page, which is a dropped row rather than a mis-ordered one. See
    /// the module docs for what counts as proof of NON NULL here.
    #[error(
        "the ORDER BY term `{column}` is not known to be NON NULL, and a \
         keyset comparison against NULL selects no rows, so the rows whose \
         `{column}` is NULL would appear on no page"
    )]
    OrderTermNullable { column: String },

    /// An `ORDER BY` term whose nullability the text does not settle either
    /// way.
    ///
    /// Distinct from [`Self::OrderTermNullable`], which says the term CAN be
    /// NULL, and the distinction is the caller's repair. A caller told that
    /// `count(*)` "is not known to be NON NULL" has nothing to fix and no way
    /// to tell a planner gap from a real hazard; a caller told which link of
    /// the proof is missing can rewrite around it, by ordering on a schema
    /// column rather than a declared one, or by lifting the term out of a
    /// derived table.
    #[error(
        "the ORDER BY term `{column}` cannot be proved NON NULL from the statement \
         text ({reason}), and a keyset comparison against NULL selects no rows, so \
         the rows whose `{column}` is NULL would appear on no page"
    )]
    OrderTermNullabilityUnknown {
        column: String,
        reason: &'static str,
    },

    /// The resume tuple does not have one value per effective term.
    #[error("the resume position has {found} values for {expected} ORDER BY terms")]
    ResumeArity { expected: usize, found: usize },

    /// A NaN or infinite resume value. Neither has a SQL literal, and NaN
    /// compares false against everything, so a page resumed at one would be
    /// empty rather than wrong-by-a-row.
    #[error("a resume value is not finite, so it has no SQL literal")]
    NonFiniteResumeValue,
}

/// One value of a resume tuple: the previous page's last row, one value per
/// effective `ORDER BY` term.
///
/// A typed value rather than caller-supplied literal text. The rewrite splices
/// these into a statement, and a `String` of SQL would put the caller in
/// control of that statement's text.
///
/// There is no NULL variant, which is deliberate: every comparison against
/// NULL is NULL, so a keyset predicate resumed at a NULL selects no rows at
/// all. A term that can be NULL cannot be paged by this form at all, which is
/// why [`PagePlanError::OrderTermNullable`] refuses one before a resume tuple
/// is ever rendered against it. [`PagePlanError::UnsupportedOrderOption`]
/// refuses the `NULLS FIRST`/`NULLS LAST` spellings on top of that, but it is
/// not what closes this hole: an omitted option still leaves NULL rows in the
/// sequence.
#[derive(Debug, Clone, PartialEq)]
pub enum ResumeValue {
    /// A signed integer column.
    Int(i64),
    /// An unsigned integer column (`generation`, `writer_seq`).
    UInt(u64),
    /// A boolean column (a declared `Bool` attribute).
    Bool(bool),
    /// A float column (`value`). NaN and the infinities are refused.
    Float(f64),
    /// A string column, rendered as a single-quoted literal with `'` doubled.
    Str(String),
    /// A nanosecond event time (`ts`, `start_ts`, `ts_ns`), rendered as an
    /// `arrow_cast` into the column's own `Timestamp(Nanosecond, None)` type
    /// so the comparison needs no coercion and no calendar formatting.
    TimestampNanos(i64),
    /// A variable-width binary column, rendered as a hex `decode`.
    Binary(Vec<u8>),
    /// A fixed-width binary column (`series_id`, `trace_id`, `span_id`),
    /// rendered as the same `decode` cast to the width of the value given.
    FixedSizeBinary(Vec<u8>),
}

impl ResumeValue {
    /// This value as a SQL literal expression.
    fn render(&self) -> Result<String, PagePlanError> {
        Ok(match self {
            ResumeValue::Int(v) => v.to_string(),
            ResumeValue::UInt(v) => v.to_string(),
            ResumeValue::Bool(v) => if *v { "TRUE" } else { "FALSE" }.to_string(),
            ResumeValue::Float(v) => {
                if !v.is_finite() {
                    return Err(PagePlanError::NonFiniteResumeValue);
                }
                // `{:?}`, not `{}`: Debug for f64 prints the shortest text that
                // round-trips to the same bits, and this repo compares floats
                // by bit pattern.
                format!("{v:?}")
            }
            ResumeValue::Str(s) => format!("'{}'", s.replace('\'', "''")),
            ResumeValue::TimestampNanos(ns) => {
                format!("arrow_cast({ns}, 'Timestamp(Nanosecond, None)')")
            }
            ResumeValue::Binary(bytes) => format!("decode('{}', 'hex')", hex::encode(bytes)),
            ResumeValue::FixedSizeBinary(bytes) => format!(
                "arrow_cast(decode('{}', 'hex'), 'FixedSizeBinary({})')",
                hex::encode(bytes),
                bytes.len()
            ),
        })
    }
}

/// Where the previous page stopped: its last row's values, in the order of the
/// [`PagePlan::order_by`] the page was taken under.
#[derive(Debug, Clone, PartialEq)]
pub struct ResumePosition {
    pub tuple: Vec<ResumeValue>,
}

impl ResumePosition {
    /// A position from one value per term.
    pub fn new(tuple: Vec<ResumeValue>) -> Self {
        ResumePosition { tuple }
    }
}

/// One page's plan: what to execute, what ordering it runs under, and whether
/// that ordering admits a strict keyset predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagePlan {
    /// The statement to execute for this page.
    pub statement: String,
    /// The effective ordering, in term order: the statement's own terms
    /// followed by whatever [`Self::tiebreak_appended`] names.
    pub order_by: Vec<OrderTerm>,
    /// The columns this planner appended to the statement's own `ORDER BY`,
    /// in the order they were appended. Empty when the statement's own
    /// ordering already determined the sequence, and when nothing could make
    /// it total.
    pub tiebreak_appended: Vec<String>,
    /// Why [`Self::order_by`] is not a total order, or `None` when it is.
    pub not_total: Option<NotTotalOrder>,
}

impl PagePlan {
    /// Whether the effective ordering uniquely determines the row sequence.
    /// True is what D5 requires before a cursor with a strict keyset predicate
    /// may be minted.
    pub fn total_order(&self) -> bool {
        self.not_total.is_none()
    }
}

/// Plan one page of `sql`, resuming after `resume` when one is given.
///
/// See the module docs for the rewrite's shape and for what total order means
/// here. Pure text-to-text: nothing is resolved, planned, or executed.
pub fn plan_page(sql: &str, resume: Option<&ResumePosition>) -> Result<PagePlan, PagePlanError> {
    // The security gate first, exactly as the executor runs it, so this
    // function never rewrites a statement the executor would refuse.
    validate(sql)?;
    let query = parse_query(sql)?;

    // Before anything else reads the `SELECT` body: a pipe operator makes that
    // body an incomplete description of the statement, and a row limit at any
    // depth re-evaluates per page, so every check below either answers about
    // the wrong rows or answers about rows that change under it.
    reject_nested_pipes_and_row_limits(&query)?;

    let target = page_target(sql)?;
    let projection = projection_of(&query);

    let mut terms = statement_order_terms(&query)?;
    for term in &terms {
        if !projection.projects(&term.column) {
            return Err(PagePlanError::OrderTermNotProjected {
                column: term.column.clone(),
            });
        }
    }

    let (tiebreak_appended, not_total) = tiebreak(&query, target, &projection, &terms);
    for column in &tiebreak_appended {
        terms.push(OrderTerm::ascending(column.clone()));
    }
    if terms.is_empty() {
        return Err(PagePlanError::NoOrdering {
            target: target.describe(),
        });
    }
    // Every effective term, the appended tiebreak included: a NULL anywhere in
    // the ordering drops the rows it covers from every page.
    let basis = schema_basis(&query, target);
    for term in &terms {
        match term_nullability(&basis, &projection, &term.column) {
            NullProof::NonNull => {}
            NullProof::Nullable => {
                return Err(PagePlanError::OrderTermNullable {
                    column: term.column.clone(),
                });
            }
            NullProof::Unproven(reason) => {
                return Err(PagePlanError::OrderTermNullabilityUnknown {
                    column: term.column.clone(),
                    reason,
                });
            }
        }
    }

    let statement = render_statement(&query, &terms, resume)?;
    Ok(PagePlan {
        statement,
        order_by: terms,
        tiebreak_appended,
        not_total,
    })
}

/// The single `Statement::Query` of `sql`. Every other shape is unreachable:
/// [`validate`] has already accepted the text, and it accepts exactly one
/// statement and only a `Statement::Query`.
fn parse_query(sql: &str) -> Result<Query, PagePlanError> {
    let statements = DFParser::parse_sql(sql).map_err(|e| ValidationError::Parse(e.to_string()))?;
    match statements.front() {
        Some(DFStatement::Statement(inner)) => match inner.as_ref() {
            Statement::Query(query) => Ok(query.as_ref().clone()),
            _ => Err(PagePlanError::Invalid(ValidationError::NotReadOnly {
                kind: "the statement",
            })),
        },
        _ => Err(PagePlanError::Invalid(ValidationError::Empty)),
    }
}

/// Refuse a pipe operator and a row limit wherever either sits, not only on
/// the outermost `Query`.
///
/// Both used to be read off the top-level `Query` alone, which made a pipe or
/// a limit on any inner query invisible: a subquery in `WHERE`, a scalar
/// subquery in the projection, a derived table in `FROM`, an arm of a set
/// operation, and a CTE all carry their own `Query`, and `Display for Query`
/// re-emits every one of them verbatim into the derived table the rewrite
/// wraps. An inner `LIMIT 5` then re-evaluates against a fresh arbitrary five
/// rows on every page, so consecutive pages of one cursor are pages of
/// different results, with `not_total: None` reported for the whole thing.
///
/// Every nested row limit is refused, including one whose own subquery is
/// totally ordered and therefore picks the same rows every time. Telling those
/// apart means reading each inner query's ordering and proving it total, which
/// is a design change; this is a correctness fix, so the conservative refusal
/// is the decision.
///
/// The pipe check is keyed on a pipe being PRESENT rather than on a list of
/// pipe kinds, so a kind a later sqlparser adds fails closed.
fn reject_nested_pipes_and_row_limits(query: &Query) -> Result<(), PagePlanError> {
    let mut found: Option<PagePlanError> = None;
    let _ = query.visit(&mut NestedShapeGuard { found: &mut found });
    match found {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

struct NestedShapeGuard<'a> {
    found: &'a mut Option<PagePlanError>,
}

impl Visitor for NestedShapeGuard<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if !query.pipe_operators.is_empty() {
            *self.found = Some(PagePlanError::PipeOperator);
            return ControlFlow::Break(());
        }
        if query.limit_clause.is_some() || query.fetch.is_some() {
            *self.found = Some(PagePlanError::RowLimitInStatement);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    /// `TOP` hangs off the `SELECT` body rather than the `Query`, and an arm
    /// of a set operation is a bare `SetExpr::Select` with no `Query` of its
    /// own, so it needs its own hook to be seen in every position.
    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<()> {
        if select.top.is_some() {
            *self.found = Some(PagePlanError::RowLimitInStatement);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// The table a page is taken over, for the row-identity question alone.
///
/// Mirrors `SqlExecutor::target_signal`'s mapping of the `FROM` clause,
/// including its CTE handling (both read `referenced_base_tables`) and its
/// refusal of a statement naming two tables. It does not reuse `TargetSignal`
/// because the distinction that matters here is not the signal: it is whether
/// the target has a row identity, and whether it has a real table at all (a
/// constant statement resolves as metrics and has neither).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageTarget {
    /// The `samples` table, the one target with a row identity.
    Samples,
    /// One of the four tables that have none.
    NoIdentity(&'static str),
    /// A statement naming no real table (`SELECT 1`, or one whose only source
    /// is a CTE over no base table).
    NoTable,
}

impl PageTarget {
    /// The table this target's rows come from, or `None` for a statement with
    /// no base table.
    fn table_name(self) -> Option<&'static str> {
        match self {
            PageTarget::Samples => Some(SAMPLES_TABLE),
            PageTarget::NoIdentity(table) => Some(table),
            PageTarget::NoTable => None,
        }
    }

    /// This target as a refusal message names it.
    fn describe(self) -> &'static str {
        self.table_name()
            .unwrap_or("a statement with no base table")
    }
}

fn page_target(sql: &str) -> Result<PageTarget, PagePlanError> {
    let tables = referenced_base_tables(sql)?;
    let named: Vec<PageTarget> = [
        (SAMPLES_TABLE, PageTarget::Samples),
        (LOGS_TABLE, PageTarget::NoIdentity(LOGS_TABLE)),
        (SPANS_TABLE, PageTarget::NoIdentity(SPANS_TABLE)),
        (ALERTS_TABLE, PageTarget::NoIdentity(ALERTS_TABLE)),
        (AUDIT_TABLE, PageTarget::NoIdentity(AUDIT_TABLE)),
    ]
    .into_iter()
    .filter(|(name, _)| tables.contains(*name))
    .map(|(_, target)| target)
    .collect();
    match named.as_slice() {
        [] => Ok(PageTarget::NoTable),
        [one] => Ok(*one),
        _ => Err(PagePlanError::CrossSignal),
    }
}

/// What one output column of a `SELECT` list is built from, as far as the
/// text says. The distinction exists for nullability: a schema lookup answers
/// for a bare column reference and for nothing else, and an expression
/// answers for itself.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputSource {
    /// A bare reference to this column of the `FROM` relation, under its own
    /// name or an alias.
    Column(String),
    /// A computed expression, carried rather than discarded: no schema
    /// describes it, but the expression itself settles some cases outright.
    /// A literal is NON NULL whatever it is selected from. Boxed: a
    /// sqlparser `Expr` is an order of magnitude larger than the other
    /// variants, and one is held per projected output name.
    Expression(Box<SqlExpr>),
    /// An output name the projection gives twice from different sources.
    /// Which of them an order term means is not readable from the text.
    Ambiguous,
}

/// What the statement projects, as far as its text says.
enum Projection {
    /// A wildcard projects every column of the target, so any column of it is
    /// available to order by.
    Wildcard,
    /// The named output columns (an alias where the item has one, the column
    /// itself otherwise), each with what it is built from. An item that is
    /// neither -- a bare expression with no alias -- contributes no name.
    Columns(BTreeMap<String, OutputSource>),
    /// The projection is not readable from the text (a set-operation body).
    Unknown,
}

impl Projection {
    /// Whether `column` is available to order by. `Wildcard` and `Unknown`
    /// both answer yes: neither carries a name list to check against, and
    /// refusing would reject a `SELECT *` and a `UNION` whose orderings are
    /// perfectly resolvable. A column that exists in neither surfaces as the
    /// executor's own plan error, which is the same answer the caller would
    /// have got for the statement it handed in.
    fn projects(&self, column: &str) -> bool {
        match self {
            Projection::Wildcard | Projection::Unknown => true,
            Projection::Columns(names) => names.contains_key(column),
        }
    }

    /// What the output column `column` is built from, as far as the text
    /// says.
    ///
    /// A wildcard answers with the name itself: every output column of a
    /// `SELECT *` is a column of the `FROM` relation under its own name. A
    /// set-operation body answers `Unreadable` rather than guessing, which is
    /// what makes it fail the nullability check instead of passing it
    /// unexamined.
    ///
    /// A [`TermSource::Column`] name is a name in the `FROM` relation, which
    /// is the target table's own column only when that relation IS the target
    /// table. [`SchemaBasis`] is what settles that, and every caller has to
    /// consult it before turning this name into a schema lookup.
    fn term_source<'a>(&'a self, column: &'a str) -> TermSource<'a> {
        match self {
            Projection::Wildcard => TermSource::Column(column),
            Projection::Unknown => TermSource::Unreadable(unproven::SET_OPERATION),
            Projection::Columns(items) => match items.get(column) {
                Some(OutputSource::Column(name)) => TermSource::Column(name.as_str()),
                Some(OutputSource::Expression(expr)) => TermSource::Expression(expr),
                Some(OutputSource::Ambiguous) => TermSource::Unreadable(unproven::AMBIGUOUS_NAME),
                None => TermSource::Unreadable(unproven::NOT_A_BARE_COLUMN),
            },
        }
    }
}

/// What an `ORDER BY` term resolves to in the projection that produced it.
enum TermSource<'a> {
    /// A bare reference to this column of the `FROM` relation.
    Column(&'a str),
    /// A computed expression, which the text carries in full.
    Expression(&'a SqlExpr),
    /// Neither, for this reason.
    Unreadable(&'static str),
}

fn projection_of(query: &Query) -> Projection {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Projection::Unknown;
    };
    let mut names: BTreeMap<String, OutputSource> = BTreeMap::new();
    let mut record = |name: String, source: OutputSource| {
        // A name the projection gives twice is ambiguous here even when both
        // sources are columns, so it degrades rather than resolving to
        // whichever item came last.
        let entry = names.entry(name).or_insert_with(|| source.clone());
        if *entry != source {
            *entry = OutputSource::Ambiguous;
        }
    };
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Projection::Wildcard;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let source = match column_of(expr) {
                    Some(name) => OutputSource::Column(name),
                    None => OutputSource::Expression(Box::new(expr.clone())),
                };
                record(ident_name(alias), source);
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(column) = column_of(expr) {
                    record(column.clone(), OutputSource::Column(column));
                }
            }
            // A multi-alias item names columns this planner does not model, so
            // it contributes no name: an ordering over one is refused rather
            // than admitted on a guess.
            SelectItem::ExprWithAliases { .. } => {}
        }
    }
    Projection::Columns(names)
}

/// Whether an output name may be resolved against the target table's public
/// schema at all.
///
/// [`Projection::source_column`] maps an output name to a name in the `FROM`
/// relation. Asking the target table's schema about that name is sound only
/// when the `FROM` relation IS that base table: otherwise the schema answers
/// about a column the value did not come from, and a NOT NULL declaration
/// there says nothing about the value being ordered on.
enum SchemaBasis {
    /// Output names resolve against this table's public schema.
    Resolvable(&'static str),
    /// They do not, for this reason, which the refusal quotes.
    Unresolvable(&'static str),
}

/// The reasons a [`PagePlanError::OrderTermNullabilityUnknown`] can carry,
/// named rather than written inline so a test pins which link of the proof
/// was missing without restating the sentence, and so two paths cannot drift
/// into two spellings of one reason.
mod unproven {
    pub(super) const NOT_A_BARE_COLUMN: &str = "it is not a bare reference to a column of the FROM relation, so no schema \
         lookup describes it";
    pub(super) const NO_BASE_TABLE: &str = "the statement has no base table";
    pub(super) const WITH_CLAUSE: &str =
        "a WITH clause can declare the target table's own name and shadow it";
    pub(super) const SET_OPERATION: &str = "a set operation's projection is not readable here";
    pub(super) const NOT_ONE_RELATION: &str = "its FROM clause is not one relation";
    pub(super) const JOINED: &str = "a join leaves an output name resolvable against more than one relation, and an \
         outer join nulls one side of it";
    pub(super) const DERIVED_RELATION: &str =
        "its FROM relation is a derived table or a table function, not the target table";
    pub(super) const OTHER_RELATION: &str = "its FROM relation is not the target table itself";
    pub(super) const GROUPING_NULLS: &str = "a ROLLUP, CUBE or GROUPING SETS grouping nulls a grouping column in its \
         super-aggregate rows";
    pub(super) const NOT_A_SCHEMA_COLUMN: &str = "it is not a column of the target table's public schema, so it is a declared \
         column whose nullability the text does not carry";
    pub(super) const NO_PUBLIC_SCHEMA: &str = "the target table has no public schema here";
    pub(super) const AMBIGUOUS_NAME: &str =
        "the projection gives that output name twice, from different sources";
    pub(super) const EXPRESSION_NOT_PROVABLE: &str = "the expression it is defined by is not one this planner proves NON NULL, and \
         SUM, MIN, MAX and AVG are NULL over empty or all-NULL input";
}

/// Whether the statement's own `FROM` relation is the target base table, so
/// that the table's public schema describes the values it projects.
///
/// The three shapes this refuses each produced a plan whose keyset predicate
/// silently dropped every row with a NULL order term, from every page, with
/// no error:
///
/// - a derived table or a CTE, where the output name is the inner query's
///   own (`SELECT * FROM (SELECT nullif(value, 0) AS ts, series_id FROM
///   samples) x ORDER BY ts` proved `ts` off `samples.ts` while the value was
///   `nullif(value, 0)`);
/// - the nullable side of an outer join, where the base column really is NOT
///   NULL and is still NULL for every unmatched row;
/// - `ROLLUP`, `CUBE` and `GROUPING SETS`, where the schema lookup is correct
///   and the grouping construct introduces the NULL in the super-aggregate
///   row.
///
/// Two of the refusals are wider than those shapes strictly need, and both
/// are deliberate. An inner or cross join is refused alongside the outer
/// ones, because an output name under a join is resolvable against more than
/// one relation and a single-table lookup cannot say which. A `WITH` clause
/// is refused even when the `FROM` names the base table, because a CTE can
/// declare that same name and shadow it.
fn schema_basis(query: &Query, target: PageTarget) -> SchemaBasis {
    let Some(table) = target.table_name() else {
        return SchemaBasis::Unresolvable(unproven::NO_BASE_TABLE);
    };
    if query.with.is_some() {
        return SchemaBasis::Unresolvable(unproven::WITH_CLAUSE);
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return SchemaBasis::Unresolvable(unproven::SET_OPERATION);
    };
    let [only] = select.from.as_slice() else {
        return SchemaBasis::Unresolvable(unproven::NOT_ONE_RELATION);
    };
    if !only.joins.is_empty() {
        return SchemaBasis::Unresolvable(unproven::JOINED);
    }
    let TableFactor::Table {
        name, args: None, ..
    } = &only.relation
    else {
        return SchemaBasis::Unresolvable(unproven::DERIVED_RELATION);
    };
    if bare_name(name).as_deref() != Some(table) {
        return SchemaBasis::Unresolvable(unproven::OTHER_RELATION);
    }
    if grouping_can_null_a_grouping_column(&select.group_by) {
        return SchemaBasis::Unresolvable(unproven::GROUPING_NULLS);
    }
    SchemaBasis::Resolvable(table)
}

/// Whether the `GROUP BY` can emit a row where a grouping column is NULL even
/// though that column is declared NOT NULL.
///
/// `ROLLUP`, `CUBE`, `GROUPING SETS` and ClickHouse's `WITH TOTALS` each add
/// a super-aggregate row whose unaggregated grouping columns are NULL. Both
/// spellings count: the modifier form (`GROUP BY a WITH ROLLUP`) and the
/// expression form (`GROUP BY ROLLUP(a)`). A plain `GROUP BY` and
/// `GROUP BY ALL` add no such row, so neither is refused here.
fn grouping_can_null_a_grouping_column(group_by: &GroupByExpr) -> bool {
    let GroupByExpr::Expressions(exprs, modifiers) = group_by else {
        return false;
    };
    !modifiers.is_empty()
        || exprs.iter().any(|expr| {
            matches!(
                expr,
                SqlExpr::Rollup(_) | SqlExpr::Cube(_) | SqlExpr::GroupingSets(_)
            )
        })
}

/// What the statement's text says about whether an effective term can be
/// NULL.
///
/// The two negative answers are kept apart because they call for different
/// repairs: `Nullable` says the term CAN be NULL and the caller has to order
/// by something else, while `Unproven` says the text does not settle it, and
/// names what would.
enum NullProof {
    /// Provably NON NULL from the text alone.
    NonNull,
    /// The target table's public schema declares this column nullable.
    Nullable,
    /// Neither proved. The string is the reason the refusal quotes.
    Unproven(&'static str),
}

/// What the text says about the effective term `column`.
///
/// There are two routes to a proof and the term takes whichever one its
/// projection offers.
///
/// A bare column reference goes through the schema, and that proof has to hold
/// end to end: the name has to be a column of the `FROM` relation, that
/// relation has to be the target base table itself (see [`schema_basis`]), and
/// the column has to be declared non-nullable in the table's public schema. A
/// declared column is absent from the static schema, and whether it exists at
/// all depends on the tenant's declarations rather than on the text, so it is
/// `Unproven` rather than either answer.
///
/// An alias over an expression is answered by
/// [`expression_is_non_null`] reading the expression itself, with no schema
/// consulted and no basis required: a literal and a `count(...)` are NON NULL
/// whatever relation they are selected from, including under an outer join or
/// a `ROLLUP`, because neither is a grouping column that a super-aggregate row
/// can null.
fn term_nullability(basis: &SchemaBasis, projection: &Projection, column: &str) -> NullProof {
    let source = match projection.term_source(column) {
        TermSource::Column(source) => source,
        TermSource::Expression(expr) => {
            return if expression_is_non_null(expr) {
                NullProof::NonNull
            } else {
                NullProof::Unproven(unproven::EXPRESSION_NOT_PROVABLE)
            };
        }
        TermSource::Unreadable(reason) => return NullProof::Unproven(reason),
    };
    let table = match basis {
        SchemaBasis::Resolvable(table) => *table,
        SchemaBasis::Unresolvable(reason) => return NullProof::Unproven(reason),
    };
    let schema = match table {
        SAMPLES_TABLE => public_schema(),
        LOGS_TABLE => logs_schema(),
        SPANS_TABLE => spans_schema(),
        ALERTS_TABLE => alerts_schema(),
        AUDIT_TABLE => audit_schema(),
        _ => return NullProof::Unproven(unproven::NO_PUBLIC_SCHEMA),
    };
    match schema.field_with_name(source) {
        Ok(field) if !field.is_nullable() => NullProof::NonNull,
        Ok(_) => NullProof::Nullable,
        Err(_) => NullProof::Unproven(unproven::NOT_A_SCHEMA_COLUMN),
    }
}

/// Whether an expression is NON NULL for every row, from the expression alone.
///
/// Deliberately narrow, and the narrowness is the point: this answers only
/// where the answer needs no schema, no statistics and no knowledge of what
/// the inputs contain. What it admits:
///
/// - a literal other than `NULL`, and other than a placeholder, whose value
///   arrives at execution time;
/// - `count(...)` in any spelling, `count(*)` and `count(DISTINCT x)`
///   included, which returns 0 rather than NULL over empty and over all-NULL
///   input;
/// - parentheses, and a unary `+` or `-`, which are NULL exactly when their
///   operand is.
///
/// `SUM`, `MIN`, `MAX` and `AVG` are NOT admitted, and this is a decision
/// rather than an omission. Each returns NULL over empty input and over input
/// that is entirely NULL, so an ordering on one can carry a NULL row that the
/// keyset predicate would drop from every page. Telling the safe uses apart
/// means knowing whether the statement is grouped and whether any group can be
/// empty or all-NULL, which is a different analysis from this one; until it
/// exists all four keep refusing.
///
/// Anything else answers `false`, which is a refusal rather than a plan. A
/// case added here has to hold for every input, not merely for the inputs a
/// caller had in mind.
fn expression_is_non_null(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Value(value) => !matches!(value.value, Value::Null | Value::Placeholder(_)),
        SqlExpr::Nested(inner) => expression_is_non_null(inner),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => expression_is_non_null(expr),
        SqlExpr::Function(function) => {
            bare_name(&function.name).is_some_and(|name| name.eq_ignore_ascii_case(COUNT_FUNCTION))
        }
        _ => false,
    }
}

/// The one aggregate this planner proves NON NULL. Lowercase: [`bare_name`]
/// lowercases an unquoted identifier.
const COUNT_FUNCTION: &str = "count";

/// The parts of a single `SELECT` body that decide whether it projects the
/// scanned rows one-for-one.
struct SelectShape<'a> {
    top: Option<()>,
    from_table: Option<&'a ObjectName>,
    joined: bool,
    distinct: Option<&'a Distinct>,
    grouped: bool,
    having: bool,
    qualify: bool,
    other_clause: bool,
}

fn shape_of<'a>(query: &'a Query) -> Option<SelectShape<'a>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let (from_table, joined) = match select.from.as_slice() {
        [only] => (
            match &only.relation {
                TableFactor::Table {
                    name, args: None, ..
                } => Some(name),
                _ => None,
            },
            !only.joins.is_empty(),
        ),
        _ => (None, select.from.len() > 1),
    };
    let grouped = match &select.group_by {
        GroupByExpr::All(_) => true,
        GroupByExpr::Expressions(exprs, modifiers) => !exprs.is_empty() || !modifiers.is_empty(),
    };
    Some(SelectShape {
        top: select.top.as_ref().map(|_| ()),
        from_table,
        joined,
        distinct: select.distinct.as_ref(),
        grouped,
        having: select.having.is_some(),
        qualify: select.qualify.is_some(),
        other_clause: !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
            || !select.lateral_views.is_empty()
            || !select.connect_by.is_empty()
            || select.prewhere.is_some()
            || !select.named_window.is_empty(),
    })
}

/// The statement's own `ORDER BY`, as effective terms.
fn statement_order_terms(query: &Query) -> Result<Vec<OrderTerm>, PagePlanError> {
    let Some(OrderBy { kind, .. }) = &query.order_by else {
        return Ok(Vec::new());
    };
    let exprs = match kind {
        OrderByKind::All(_) => return Err(PagePlanError::OrderByAll),
        OrderByKind::Expressions(exprs) => exprs,
    };
    let mut terms = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let Some(column) = column_of(&expr.expr) else {
            return Err(PagePlanError::OrderTermNotColumn {
                term: expr.expr.to_string(),
            });
        };
        if expr.options.nulls_first.is_some() {
            return Err(PagePlanError::UnsupportedOrderOption {
                column,
                option: "NULLS FIRST/LAST",
            });
        }
        if expr.with_fill.is_some() {
            return Err(PagePlanError::UnsupportedOrderOption {
                column,
                option: "WITH FILL",
            });
        }
        terms.push(OrderTerm {
            column,
            descending: expr.options.asc == Some(false),
        });
    }
    Ok(terms)
}

/// The tiebreak columns to append, and why the result is not total when it is
/// not.
///
/// The two are decided together because they are one question: a target with a
/// row identity whose columns are all projected gets the missing ones
/// appended and is total; every other case appends nothing and names its
/// reason. Appending a column that cannot make the ordering unique would put
/// a term in the pinned `ORDER BY` that buys the caller nothing and costs a
/// sort.
fn tiebreak(
    query: &Query,
    target: PageTarget,
    projection: &Projection,
    terms: &[OrderTerm],
) -> (Vec<String>, Option<NotTotalOrder>) {
    let identity: &[&str] = match target {
        PageTarget::Samples => &SAMPLES_ROW_IDENTITY,
        PageTarget::NoIdentity(table) => {
            return (Vec::new(), Some(NotTotalOrder::NoRowIdentity { table }));
        }
        PageTarget::NoTable => {
            return (
                Vec::new(),
                Some(NotTotalOrder::ShapeNotIdentityPreserving {
                    shape: "no base table",
                }),
            );
        }
    };

    if let Some(shape) = non_identity_shape(query, target) {
        return (
            Vec::new(),
            Some(NotTotalOrder::ShapeNotIdentityPreserving { shape }),
        );
    }

    let mut missing = Vec::new();
    let mut append = Vec::new();
    for column in identity {
        if !projection.projects(column) {
            missing.push((*column).to_string());
            continue;
        }
        if !terms.iter().any(|term| term.column == *column) {
            append.push((*column).to_string());
        }
    }
    if !missing.is_empty() {
        return (
            Vec::new(),
            Some(NotTotalOrder::TiebreakNotProjected { missing }),
        );
    }
    (append, None)
}

/// The first shape reason this statement does not project its target's rows
/// one-for-one, or `None` when it does.
///
/// Conservative by construction: it names a reason for everything except a
/// single `SELECT` straight off the target table with no CTE, no join, no
/// grouping, and no dialect clause that reshapes rows. A statement this
/// passes is one whose result rows are the scan's rows, so the scan's row
/// identity is the result's.
///
/// A pipe operator and a `TOP` clause are refused outright by [`plan_page`]
/// before this runs, so neither reason can reach a returned plan. They are
/// named here anyway: this classification has to be complete on its own
/// reading, not only in combination with what its one caller happens to check
/// first.
fn non_identity_shape(query: &Query, target: PageTarget) -> Option<&'static str> {
    if query.with.is_some() {
        return Some("a WITH clause");
    }
    if !query.pipe_operators.is_empty() {
        return Some("a pipe operator");
    }
    let Some(shape) = shape_of(query) else {
        return Some("a set operation");
    };
    if shape.top.is_some() {
        return Some("a TOP clause");
    }
    if shape.joined {
        return Some("a join");
    }
    if shape.distinct.is_some() {
        return Some("DISTINCT");
    }
    if shape.grouped {
        return Some("GROUP BY");
    }
    if shape.having {
        return Some("HAVING");
    }
    if shape.qualify {
        return Some("QUALIFY");
    }
    if shape.other_clause {
        return Some("a row-reshaping clause");
    }
    match (shape.from_table, target.table_name()) {
        // The `FROM` has to be the target table itself: a derived table or a
        // table function projects rows this planner cannot reason about, and
        // `referenced_base_tables` would still report the target underneath it.
        (Some(name), Some(table)) if bare_name(name).as_deref() == Some(table) => None,
        _ => Some("a FROM clause that is not the target table"),
    }
}

/// Render the page statement.
fn render_statement(
    query: &Query,
    terms: &[OrderTerm],
    resume: Option<&ResumePosition>,
) -> Result<String, PagePlanError> {
    // The caller's own `ORDER BY` is dropped from the derived table: the
    // effective ordering is applied once, outside, where the keyset predicate
    // is. Leaving it would order the same rows twice.
    let mut inner = query.clone();
    inner.order_by = None;

    let ordering = terms
        .iter()
        .map(OrderTerm::render)
        .collect::<Vec<String>>()
        .join(", ");
    let filter = match resume {
        Some(position) => format!(" WHERE {}", keyset_predicate(terms, &position.tuple)?),
        None => String::new(),
    };
    Ok(format!(
        "SELECT * FROM ({inner}) AS {PAGE_ALIAS}{filter} ORDER BY {ordering}"
    ))
}

/// The strict lexicographic keyset predicate for `terms` at `tuple`.
fn keyset_predicate(terms: &[OrderTerm], tuple: &[ResumeValue]) -> Result<String, PagePlanError> {
    if terms.len() != tuple.len() {
        return Err(PagePlanError::ResumeArity {
            expected: terms.len(),
            found: tuple.len(),
        });
    }
    let values = tuple
        .iter()
        .map(ResumeValue::render)
        .collect::<Result<Vec<String>, PagePlanError>>()?;
    let mut disjuncts = Vec::with_capacity(terms.len());
    for (index, term) in terms.iter().enumerate() {
        let mut conjuncts = Vec::with_capacity(index + 1);
        for (earlier, value) in terms.iter().zip(&values).take(index) {
            conjuncts.push(format!("{} = {}", quote_ident(&earlier.column), value));
        }
        let operator = if term.descending { "<" } else { ">" };
        let value = values.get(index).map_or("", String::as_str);
        conjuncts.push(format!(
            "{} {} {}",
            quote_ident(&term.column),
            operator,
            value
        ));
        disjuncts.push(format!("({})", conjuncts.join(" AND ")));
    }
    Ok(format!("({})", disjuncts.join(" OR ")))
}

/// The column an expression names, or `None` when it is not a column
/// reference. A qualified reference (`logs.ts`) contributes its last part,
/// which is the output column name.
fn column_of(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident_name(ident)),
        SqlExpr::CompoundIdentifier(parts) => parts.last().map(ident_name),
        _ => None,
    }
}

/// An identifier's name. An unquoted identifier is lowercased, matching how
/// the planner resolves one; a quoted identifier keeps its case.
fn ident_name(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// A table's bare name, lowercased, or `None` when it is qualified or built by
/// a dialect-specific function part.
fn bare_name(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [only] => only.as_ident().map(ident_name),
        _ => None,
    }
}

/// `column` as it is written into the page statement: always double-quoted,
/// with `"` doubled.
///
/// Unconditionally, with no bare spelling for a plain-looking name. A name
/// that is all lowercase, digits and underscores can still be a reserved word
/// (`select`, `order`, `from`), reachable here through a quoted alias, and
/// emitting one of those bare re-parses it as syntax rather than as the
/// column: the ordering and the keyset predicate would then be about
/// something other than the row. Quoting every name is what makes the rewrite
/// independent of the keyword list of whatever dialect parses it back.
fn quote_ident(column: &str) -> String {
    format!("\"{}\"", column.replace('"', "\"\""))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

    /// The `series_id` value used in every keyset assertion: sixteen `0xab`
    /// bytes, which is a canonical series id's width (ADR-0005).
    const SERIES_ID: [u8; 16] = [0xab; 16];

    /// Its hex rendering, spelled out so the assertions below pin the exact
    /// literal text rather than recomputing it the way the code under test
    /// does.
    const SERIES_ID_HEX: &str = "abababababababababababababababab";

    /// ADR-1374 D5: `ravel_query_sql` mints a cursor only when the ORDER BY
    /// plus an appended deterministic tiebreak is a total order. A `samples`
    /// statement ordered by `ts` alone is not unique (one timestamp, many
    /// series), so the planner appends the rest of the row identity and reports
    /// a total order.
    #[test]
    fn appends_the_row_identity_tiebreak_and_reports_a_total_order() {
        let plan = plan_page("SELECT * FROM samples ORDER BY ts", None).expect("planned");

        assert_eq!(plan.tiebreak_appended, vec!["series_id".to_string()]);
        assert_eq!(
            plan.order_by,
            vec![
                OrderTerm::ascending("ts"),
                OrderTerm::ascending("series_id")
            ],
        );
        assert_eq!(plan.not_total, None);
        assert!(plan.total_order());
        assert_eq!(
            plan.statement,
            "SELECT * FROM (SELECT * FROM samples) AS ravel_page \
             ORDER BY \"ts\" ASC, \"series_id\" ASC",
        );

        // A statement already ordered by the whole identity needs no tiebreak,
        // and the appended list is empty rather than a repeat of its terms.
        let complete =
            plan_page("SELECT * FROM samples ORDER BY series_id DESC, ts", None).expect("planned");
        assert_eq!(complete.tiebreak_appended, Vec::<String>::new());
        assert_eq!(
            complete.order_by,
            vec![
                OrderTerm::descending("series_id"),
                OrderTerm::ascending("ts"),
            ],
        );
        assert!(complete.total_order());
        assert_eq!(
            complete.statement,
            "SELECT * FROM (SELECT * FROM samples) AS ravel_page \
             ORDER BY \"series_id\" DESC, \"ts\" ASC",
        );
    }

    /// The two ways an ordering cannot be made unique, each reported rather
    /// than guessed at. `logs` has no row identity, so no tiebreak exists to
    /// append; a grouped `samples` statement does not project the scan's rows
    /// one-for-one, so the identity that exists does not carry to its result.
    /// Both are reported as not-total plans, not refusals: D5 pages them under
    /// the equal-group rule instead of a keyset predicate.
    #[test]
    fn reports_not_total_for_an_ordering_it_cannot_make_unique() {
        let logs = plan_page("SELECT * FROM logs ORDER BY ts", None).expect("planned");
        assert_eq!(
            logs.not_total,
            Some(NotTotalOrder::NoRowIdentity { table: "logs" }),
        );
        assert!(!logs.total_order());
        assert_eq!(logs.tiebreak_appended, Vec::<String>::new());
        assert_eq!(logs.order_by, vec![OrderTerm::ascending("ts")]);
        assert_eq!(
            logs.statement,
            "SELECT * FROM (SELECT * FROM logs) AS ravel_page ORDER BY \"ts\" ASC",
        );

        let grouped = plan_page(
            "SELECT ts, count(*) AS hits FROM samples GROUP BY ts ORDER BY ts",
            None,
        )
        .expect("planned");
        assert_eq!(
            grouped.not_total,
            Some(NotTotalOrder::ShapeNotIdentityPreserving { shape: "GROUP BY" }),
        );
        assert!(!grouped.total_order());
        assert_eq!(grouped.tiebreak_appended, Vec::<String>::new());
        assert_eq!(grouped.order_by, vec![OrderTerm::ascending("ts")]);

        // A projection that omits an identity column is the third case: the
        // columns exist on the table but a page's own rows would not carry the
        // values the next cursor position needs.
        let narrow = plan_page("SELECT ts FROM samples ORDER BY ts", None).expect("planned");
        assert_eq!(
            narrow.not_total,
            Some(NotTotalOrder::TiebreakNotProjected {
                missing: vec!["series_id".to_string()],
            }),
        );
        assert!(!narrow.total_order());
        assert_eq!(narrow.tiebreak_appended, Vec::<String>::new());
    }

    /// A resume position becomes the strict lexicographic keyset predicate,
    /// with the comparison flipped per DESC term. Asserted as exact statement
    /// text: the whole point of the planner is the text it emits.
    #[test]
    fn a_resume_position_becomes_the_expected_keyset_predicate() {
        let resume = ResumePosition::new(vec![
            ResumeValue::TimestampNanos(500),
            ResumeValue::FixedSizeBinary(SERIES_ID.to_vec()),
        ]);

        let plan = plan_page("SELECT * FROM samples ORDER BY ts", Some(&resume)).expect("planned");
        assert_eq!(
            plan.statement,
            format!(
                "SELECT * FROM (SELECT * FROM samples) AS ravel_page \
                 WHERE ((\"ts\" > arrow_cast(500, 'Timestamp(Nanosecond, None)')) \
                 OR (\"ts\" = arrow_cast(500, 'Timestamp(Nanosecond, None)') \
                 AND \"series_id\" > arrow_cast(decode('{SERIES_ID_HEX}', 'hex'), \
                 'FixedSizeBinary(16)'))) \
                 ORDER BY \"ts\" ASC, \"series_id\" ASC"
            ),
        );

        // Descending on the leading term flips only that term's comparison;
        // the equality conjunct and the appended ascending tiebreak are
        // unchanged.
        let descending =
            plan_page("SELECT * FROM samples ORDER BY ts DESC", Some(&resume)).expect("planned");
        assert_eq!(
            descending.statement,
            format!(
                "SELECT * FROM (SELECT * FROM samples) AS ravel_page \
                 WHERE ((\"ts\" < arrow_cast(500, 'Timestamp(Nanosecond, None)')) \
                 OR (\"ts\" = arrow_cast(500, 'Timestamp(Nanosecond, None)') \
                 AND \"series_id\" > arrow_cast(decode('{SERIES_ID_HEX}', 'hex'), \
                 'FixedSizeBinary(16)'))) \
                 ORDER BY \"ts\" DESC, \"series_id\" ASC"
            ),
        );

        // The caller's own predicate survives inside the derived table, and
        // the keyset lands outside it.
        let filtered = plan_page(
            "SELECT * FROM samples WHERE value > 1 ORDER BY ts",
            Some(&resume),
        )
        .expect("planned");
        assert_eq!(
            filtered.statement,
            format!(
                "SELECT * FROM (SELECT * FROM samples WHERE value > 1) AS ravel_page \
                 WHERE ((\"ts\" > arrow_cast(500, 'Timestamp(Nanosecond, None)')) \
                 OR (\"ts\" = arrow_cast(500, 'Timestamp(Nanosecond, None)') \
                 AND \"series_id\" > arrow_cast(decode('{SERIES_ID_HEX}', 'hex'), \
                 'FixedSizeBinary(16)'))) \
                 ORDER BY \"ts\" ASC, \"series_id\" ASC"
            ),
        );
    }

    /// A resume tuple that does not match the effective terms is refused
    /// rather than truncated: the effective terms include whatever tiebreak
    /// was appended, so the arity a caller has to supply is the plan's, not
    /// the statement's.
    #[test]
    fn a_resume_tuple_of_the_wrong_arity_is_refused() {
        let err = plan_page(
            "SELECT * FROM samples ORDER BY ts",
            Some(&ResumePosition::new(vec![ResumeValue::TimestampNanos(500)])),
        )
        .expect_err("refused");
        assert_eq!(
            err,
            PagePlanError::ResumeArity {
                expected: 2,
                found: 1,
            },
        );
    }

    /// Each resume value renders as the literal form its column type compares
    /// against without coercion, and a string closes over its own quote.
    #[test]
    fn resume_values_render_as_their_sql_literals() {
        let plan = plan_page(
            "SELECT * FROM logs ORDER BY severity_text, ts",
            Some(&ResumePosition::new(vec![
                ResumeValue::Str("it's here".to_string()),
                ResumeValue::Int(-7),
            ])),
        )
        .expect("planned");
        assert_eq!(
            plan.statement,
            "SELECT * FROM (SELECT * FROM logs) AS ravel_page \
             WHERE ((\"severity_text\" > 'it''s here') \
             OR (\"severity_text\" = 'it''s here' AND \"ts\" > -7)) \
             ORDER BY \"severity_text\" ASC, \"ts\" ASC",
        );

        // The unsigned variant, on the one non-nullable UInt column any public
        // schema declares: `alerts.writer_seq`.
        let unsigned = plan_page(
            "SELECT * FROM alerts ORDER BY writer_seq",
            Some(&ResumePosition::new(vec![ResumeValue::UInt(
                18_446_744_073_709_551_615,
            )])),
        )
        .expect("planned");
        assert_eq!(
            unsigned.statement,
            "SELECT * FROM (SELECT * FROM alerts) AS ravel_page \
             WHERE ((\"writer_seq\" > 18446744073709551615)) \
             ORDER BY \"writer_seq\" ASC",
        );

        let floats = plan_page(
            "SELECT * FROM samples ORDER BY value, ts, series_id",
            Some(&ResumePosition::new(vec![
                ResumeValue::Float(-0.5),
                ResumeValue::TimestampNanos(1),
                ResumeValue::FixedSizeBinary(SERIES_ID.to_vec()),
            ])),
        )
        .expect("planned");
        assert!(
            floats.statement.contains("\"value\" > -0.5"),
            "unexpected float literal in {}",
            floats.statement
        );

        let err = plan_page(
            "SELECT * FROM samples ORDER BY value, ts, series_id",
            Some(&ResumePosition::new(vec![
                ResumeValue::Float(f64::NAN),
                ResumeValue::TimestampNanos(1),
                ResumeValue::FixedSizeBinary(SERIES_ID.to_vec()),
            ])),
        )
        .expect_err("refused");
        assert_eq!(err, PagePlanError::NonFiniteResumeValue);
    }

    /// Every [`ResumeValue`] variant's exact rendered literal, including the
    /// three no statement-level assertion can reach.
    ///
    /// `Bool` and `Binary` describe declared attribute columns, and `UInt`
    /// beyond `alerts.writer_seq` likewise: a declared column is nullable, so
    /// [`PagePlanError::OrderTermNullable`] refuses an ordering on one and no
    /// page statement can carry the literal. The literal is still this
    /// module's contract with whatever mints a cursor, so it is pinned here
    /// rather than left unasserted until a NULL-aware ordering makes those
    /// columns pageable.
    #[test]
    fn every_resume_variant_renders_its_exact_sql_literal() {
        let cases: Vec<(ResumeValue, String)> = vec![
            (ResumeValue::Int(-7), "-7".to_string()),
            (ResumeValue::Int(0), "0".to_string()),
            (ResumeValue::UInt(0), "0".to_string()),
            (
                ResumeValue::UInt(18_446_744_073_709_551_615),
                "18446744073709551615".to_string(),
            ),
            (ResumeValue::Bool(true), "TRUE".to_string()),
            (ResumeValue::Bool(false), "FALSE".to_string()),
            (ResumeValue::Float(-0.5), "-0.5".to_string()),
            (ResumeValue::Float(-0.0), "-0.0".to_string()),
            (
                ResumeValue::Str("it's here".to_string()),
                "'it''s here'".to_string(),
            ),
            (
                ResumeValue::TimestampNanos(500),
                "arrow_cast(500, 'Timestamp(Nanosecond, None)')".to_string(),
            ),
            (
                ResumeValue::Binary(Vec::new()),
                "decode('', 'hex')".to_string(),
            ),
            (
                ResumeValue::Binary(vec![0x00, 0xde, 0xad, 0xff]),
                "decode('00deadff', 'hex')".to_string(),
            ),
            (
                ResumeValue::FixedSizeBinary(vec![0x01, 0x02]),
                "arrow_cast(decode('0102', 'hex'), 'FixedSizeBinary(2)')".to_string(),
            ),
            (
                ResumeValue::FixedSizeBinary(SERIES_ID.to_vec()),
                format!("arrow_cast(decode('{SERIES_ID_HEX}', 'hex'), 'FixedSizeBinary(16)')"),
            ),
        ];
        for (value, expected) in &cases {
            assert_eq!(
                &value.render().expect("rendered"),
                expected,
                "unexpected literal for {value:?}"
            );
        }
    }

    /// Everything the planner refuses outright, with the reason each carries.
    /// A refusal is the deliverable for these: a page planned from any of them
    /// would either return rows past a bound the caller set or resume at a
    /// position it cannot reproduce.
    #[test]
    fn refuses_the_statements_that_have_no_deterministic_page() {
        let cases: Vec<(&str, PagePlanError)> = vec![
            (
                "SELECT * FROM samples ORDER BY ts LIMIT 10",
                PagePlanError::RowLimitInStatement,
            ),
            (
                "SELECT * FROM samples ORDER BY ts OFFSET 10 ROWS",
                PagePlanError::RowLimitInStatement,
            ),
            // A `TOP` clause is a row limit whatever the projection is. The
            // wildcard spelling used to slip the check and be planned as a
            // not-total page that re-applied the caller's own cap per page.
            (
                "SELECT TOP 5 * FROM samples ORDER BY ts",
                PagePlanError::RowLimitInStatement,
            ),
            (
                "SELECT TOP 5 ts, series_id FROM samples ORDER BY ts",
                PagePlanError::RowLimitInStatement,
            ),
            // Pipe operators. The parser's dialect accepts them and `Display`
            // re-emits them, so a pipe not read here survives into the derived
            // table with every guard above it satisfied by the SELECT body.
            (
                "SELECT * FROM samples |> LIMIT 10",
                PagePlanError::PipeOperator,
            ),
            (
                "SELECT * FROM samples ORDER BY ts |> LIMIT 10",
                PagePlanError::PipeOperator,
            ),
            (
                "SELECT * FROM samples ORDER BY ts |> UNION ALL (SELECT * FROM samples)",
                PagePlanError::PipeOperator,
            ),
            (
                "SELECT * FROM samples ORDER BY ts |> JOIN samples AS s2 ON true",
                PagePlanError::PipeOperator,
            ),
            // Nullable ordering terms: a projected expression whose
            // nullability no schema lookup answers, and a column the schema
            // declares nullable outright. The two carry different refusals,
            // because the repairs differ.
            (
                "SELECT nullif(value, 0) AS v, ts, series_id FROM samples ORDER BY v",
                PagePlanError::OrderTermNullabilityUnknown {
                    column: "v".to_string(),
                    reason: unproven::EXPRESSION_NOT_PROVABLE,
                },
            ),
            (
                "SELECT * FROM logs ORDER BY trace_id, ts",
                PagePlanError::OrderTermNullable {
                    column: "trace_id".to_string(),
                },
            ),
            (
                "SELECT * FROM samples ORDER BY ts + 1",
                PagePlanError::OrderTermNotColumn {
                    term: "ts + 1".to_string(),
                },
            ),
            (
                "SELECT ts, series_id FROM samples ORDER BY value",
                PagePlanError::OrderTermNotProjected {
                    column: "value".to_string(),
                },
            ),
            (
                "SELECT * FROM samples ORDER BY ts NULLS LAST",
                PagePlanError::UnsupportedOrderOption {
                    column: "ts".to_string(),
                    option: "NULLS FIRST/LAST",
                },
            ),
            (
                "SELECT * FROM logs JOIN samples ON logs.ts = samples.ts ORDER BY logs.ts",
                PagePlanError::CrossSignal,
            ),
            (
                "SELECT * FROM logs",
                PagePlanError::NoOrdering { target: "logs" },
            ),
            (
                "SELECT 1",
                PagePlanError::NoOrdering {
                    target: "a statement with no base table",
                },
            ),
        ];
        for (sql, expected) in cases {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(err, expected, "unexpected refusal for {sql:?}");
        }
    }

    /// The validation gate runs first and unchanged, so a statement the
    /// executor would refuse is never rewritten into a pageable one.
    #[test]
    fn refuses_what_the_read_only_gate_refuses() {
        let err = plan_page("DELETE FROM samples", None).expect_err("refused");
        assert!(
            matches!(err, PagePlanError::Invalid(_)),
            "expected a validation refusal, got {err:?}"
        );
    }

    /// Every pipe operator is refused, whatever the pipe carries.
    ///
    /// The first three are the demonstrated defects of the first cut of this
    /// module: a pipe row limit was planned with `total_order() == true` and
    /// the caller's own cap re-applied per page, and a pipe union or join
    /// reported a total order while the tiebreak columns were no longer a key.
    /// Every guard above them was satisfied because they all read the `SELECT`
    /// body, which a pipe runs after. The rest are here because the refusal
    /// has to rest on the presence of a pipe rather than on a list of kinds:
    /// one that only reprojects or renames still makes the body an incomplete
    /// description of the rows, and a kind a later sqlparser adds has to be
    /// refused without this test being edited.
    #[test]
    fn refuses_every_pipe_operator_whatever_it_carries() {
        let cases = [
            "SELECT * FROM samples ORDER BY ts |> LIMIT 10",
            "SELECT * FROM samples ORDER BY ts |> UNION ALL (SELECT * FROM samples)",
            "SELECT * FROM samples ORDER BY ts |> JOIN samples AS s2 ON true",
            "SELECT * FROM samples ORDER BY ts |> INTERSECT DISTINCT (SELECT * FROM samples)",
            "SELECT * FROM samples ORDER BY ts |> EXCEPT DISTINCT (SELECT * FROM samples)",
            "SELECT * FROM samples ORDER BY ts |> AGGREGATE count(*)",
            "SELECT * FROM samples ORDER BY ts |> WHERE value > 1",
            "SELECT * FROM samples ORDER BY ts |> ORDER BY value",
            "SELECT * FROM samples ORDER BY ts |> SELECT ts",
            "SELECT * FROM samples ORDER BY ts |> DROP value",
            "SELECT * FROM samples ORDER BY ts |> AS renamed",
            "SELECT * FROM samples |> LIMIT 10",
        ];
        for sql in cases {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(err, PagePlanError::PipeOperator, "unexpected for {sql:?}");
            assert_eq!(
                err.to_string(),
                "a statement using a pipe operator cannot be paged; a pipe \
                 reshapes the rows after the SELECT body a page is planned from",
            );
        }
    }

    /// A pipe operator is refused wherever it sits, not only on the outermost
    /// `Query`.
    ///
    /// The five positions below each carry their own `Query`, and the guard
    /// used to read the top-level one alone: every one of these planned, with
    /// the pipe re-emitted verbatim into the derived table by `Display`. The
    /// `WHERE`-subquery case is the demonstrated defect -- `SELECT * FROM
    /// samples WHERE series_id IN (SELECT series_id FROM samples |> LIMIT 5)
    /// ORDER BY ts` planned with `not_total: None`, so the tool minted a
    /// cursor over an inner relation that re-picks five arbitrary series on
    /// every page.
    #[test]
    fn refuses_a_pipe_at_every_nesting_depth() {
        let cases = [
            // A subquery in WHERE.
            "SELECT * FROM samples WHERE series_id IN \
             (SELECT series_id FROM samples |> LIMIT 5) ORDER BY ts",
            // A scalar subquery in the projection.
            "SELECT ts, series_id, (SELECT max(value) FROM samples |> LIMIT 1) AS m \
             FROM samples ORDER BY ts",
            // A derived table in FROM.
            "SELECT * FROM (SELECT ts, series_id FROM samples |> WHERE value > 1) AS x \
             ORDER BY ts",
            // An arm of a set operation.
            "SELECT ts FROM samples UNION ALL (SELECT ts FROM samples |> LIMIT 5) ORDER BY ts",
            // A CTE in a WITH clause.
            "WITH c AS (SELECT ts, series_id FROM samples |> LIMIT 5) \
             SELECT * FROM c ORDER BY ts",
        ];
        for sql in cases {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(err, PagePlanError::PipeOperator, "unexpected for {sql:?}");
        }
    }

    /// A row limit is refused wherever it sits, for the same reason and by the
    /// same walk.
    ///
    /// An inner `LIMIT 5` with no ordering of its own selects five arbitrary
    /// rows, and it is re-evaluated on every page because the wrap re-emits
    /// it: page 2 resumes past page 1's last row of a relation that no longer
    /// contains the same rows. The refusal covers a nested limit that WOULD be
    /// deterministic too; see [`reject_nested_pipes_and_row_limits`].
    #[test]
    fn refuses_a_row_limit_at_every_nesting_depth() {
        let cases = [
            // A subquery in WHERE.
            "SELECT * FROM samples WHERE series_id IN \
             (SELECT series_id FROM samples LIMIT 5) ORDER BY ts",
            // A scalar subquery in the projection.
            "SELECT ts, series_id, (SELECT max(value) FROM samples LIMIT 1) AS m \
             FROM samples ORDER BY ts",
            // A derived table in FROM, in all three spellings a row cap has.
            "SELECT * FROM (SELECT ts, series_id FROM samples LIMIT 5) AS x ORDER BY ts",
            "SELECT * FROM (SELECT ts, series_id FROM samples OFFSET 5 ROWS) AS x ORDER BY ts",
            "SELECT * FROM (SELECT TOP 5 ts, series_id FROM samples) AS x ORDER BY ts",
            // An arm of a set operation, as a Query operand and as a bare
            // SELECT body carrying a TOP.
            "SELECT ts FROM samples UNION ALL (SELECT ts FROM samples LIMIT 5) ORDER BY ts",
            "SELECT ts FROM samples UNION ALL SELECT TOP 5 ts FROM samples ORDER BY ts",
            // A CTE in a WITH clause.
            "WITH c AS (SELECT ts, series_id FROM samples LIMIT 5) SELECT * FROM c ORDER BY ts",
        ];
        for sql in cases {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::RowLimitInStatement,
                "unexpected for {sql:?}"
            );
        }
    }

    /// An ordering term that can be NULL is refused rather than served as if
    /// it were sound.
    ///
    /// Refusing the `NULLS FIRST`/`NULLS LAST` spellings does not cover this:
    /// omitting the option leaves the NULL rows in the sequence at a session
    /// default's position, and every keyset disjunct is NULL for those rows,
    /// so they land on no page at all. That is a dropped row on both the total
    /// and the not-total path, because the keyset predicate is what resumes
    /// both. Proof of NON NULL has to come from the statement's own text, and
    /// it does for a bare reference to a column the target's public schema
    /// declares non-nullable and for nothing else.
    ///
    /// These are the cases where the schema ANSWERS, and answers nullable.
    /// The cases where it cannot answer carry
    /// [`PagePlanError::OrderTermNullabilityUnknown`] instead and are pinned
    /// by the tests below; the split matters because only these have a repair
    /// the caller can make from the message alone.
    #[test]
    fn refuses_an_ordering_term_that_can_be_null() {
        let cases: Vec<(&str, &str)> = vec![
            // Columns the schemas declare nullable, one per table that has
            // one.
            ("SELECT * FROM logs ORDER BY span_id, ts", "span_id"),
            ("SELECT * FROM spans ORDER BY service_name", "service_name"),
            ("SELECT * FROM alerts ORDER BY alert_id", "alert_id"),
        ];
        for (sql, column) in cases {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::OrderTermNullable {
                    column: column.to_string(),
                },
                "unexpected refusal for {sql:?}"
            );
            assert_eq!(
                err.to_string(),
                format!(
                    "the ORDER BY term `{column}` is not known to be NON NULL, \
                     and a keyset comparison against NULL selects no rows, so \
                     the rows whose `{column}` is NULL would appear on no page"
                ),
            );
        }

        // The cases the schema cannot answer for. Each names the link of the
        // proof that is missing, so a caller can tell a planner gap from a
        // column that really can be NULL.
        let unproven_cases: Vec<(&str, &str, &str)> = vec![
            // A projected expression: the case reachable on the TOTAL path.
            // This used to report `total_order() == true` while every keyset
            // disjunct was NULL for the rows `nullif` nulled out.
            (
                "SELECT nullif(value, 0) AS v, ts, series_id FROM samples ORDER BY v",
                "v",
                unproven::EXPRESSION_NOT_PROVABLE,
            ),
            // An output name the projection gives twice from different
            // columns: both are columns, but which one the term means is not
            // readable from the text, so the nullable one cannot be ruled out.
            (
                "SELECT ts AS a, trace_id AS a FROM logs ORDER BY a",
                "a",
                unproven::AMBIGUOUS_NAME,
            ),
            // A set-operation body carries no readable projection at all.
            (
                "SELECT ts FROM logs UNION ALL SELECT ts FROM logs ORDER BY ts",
                "ts",
                unproven::SET_OPERATION,
            ),
        ];
        for (sql, column, reason) in unproven_cases {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::OrderTermNullabilityUnknown {
                    column: column.to_string(),
                    reason,
                },
                "unexpected refusal for {sql:?}"
            );
            assert_eq!(
                err.to_string(),
                format!(
                    "the ORDER BY term `{column}` cannot be proved NON NULL from the \
                     statement text ({reason}), and a keyset comparison against NULL \
                     selects no rows, so the rows whose `{column}` is NULL would \
                     appear on no page"
                ),
            );
        }

        // The non-nullable columns of those same tables still page, including
        // through an alias over one, so the refusal is about nullability and
        // not about the table or about aliasing.
        for sql in [
            "SELECT * FROM logs ORDER BY ts, observed_ts, severity_num, body, flags",
            "SELECT * FROM spans ORDER BY trace_id, span_id, name, start_ts, duration_ns",
            "SELECT * FROM alerts ORDER BY ts_ns, writer_id, writer_epoch, writer_seq",
            "SELECT * FROM audit ORDER BY ts_ns, severity_text, body",
            "SELECT body AS message, ts FROM logs ORDER BY message, ts",
        ] {
            let plan = plan_page(sql, None);
            assert!(plan.is_ok(), "{sql:?} should plan, got {plan:?}");
        }
    }

    /// A wildcard over a derived table or a CTE resolves against the inner
    /// query's output names, not the base table's schema, so no schema lookup
    /// proves anything about it.
    ///
    /// The demonstrated defect: `SELECT * FROM (SELECT nullif(value, 0) AS
    /// ts, series_id FROM samples) x ORDER BY ts` planned. `referenced_base_
    /// tables` still reported `samples` underneath the derived table, the
    /// wildcard answered `ts` with the name itself, and `samples.ts` is
    /// declared NOT NULL, so the term was proved non-null while the value
    /// being ordered on was `nullif(value, 0)`. Every row where `value` is 0
    /// was dropped from every page by the keyset predicate, with no error.
    #[test]
    fn refuses_a_wildcard_over_a_derived_table_or_cte() {
        let derived = plan_page(
            "SELECT * FROM (SELECT nullif(value, 0) AS ts, series_id FROM samples) AS x \
             ORDER BY ts",
            None,
        )
        .expect_err("refused");
        assert_eq!(
            derived,
            PagePlanError::OrderTermNullabilityUnknown {
                column: "ts".to_string(),
                reason: unproven::DERIVED_RELATION,
            },
        );

        let cte = plan_page(
            "WITH x AS (SELECT nullif(value, 0) AS ts, series_id FROM samples) \
             SELECT * FROM x ORDER BY ts",
            None,
        )
        .expect_err("refused");
        assert_eq!(
            cte,
            PagePlanError::OrderTermNullabilityUnknown {
                column: "ts".to_string(),
                reason: unproven::WITH_CLAUSE,
            },
        );

        // A CTE that shadows the target table's own name is the same hole
        // spelled so the `FROM` relation reads as the base table. It refuses
        // one step earlier: `referenced_base_tables` subtracts a CTE-declared
        // name, so the statement resolves to no base table at all.
        let shadowing = plan_page(
            "WITH samples AS (SELECT nullif(value, 0) AS ts, series_id FROM samples) \
             SELECT * FROM samples ORDER BY ts",
            None,
        )
        .expect_err("refused");
        assert_eq!(
            shadowing,
            PagePlanError::OrderTermNullabilityUnknown {
                column: "ts".to_string(),
                reason: unproven::NO_BASE_TABLE,
            },
        );
    }

    /// A term aliased off the nullable side of an outer join is NULL for
    /// every unmatched row, whatever the base column's schema says.
    ///
    /// `logs.body` is declared NOT NULL, and `b.body` under a `LEFT JOIN` is
    /// still NULL for every left row with no match. The schema lookup was
    /// correct about `logs.body` and wrong about the value.
    #[test]
    fn refuses_a_term_from_the_nullable_side_of_an_outer_join() {
        for join in ["LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"] {
            let sql = format!(
                "SELECT a.ts AS ts, b.body AS b_body FROM logs AS a \
                 {join} logs AS b ON a.ts = b.ts ORDER BY b_body, ts"
            );
            let err = plan_page(&sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::OrderTermNullabilityUnknown {
                    column: "b_body".to_string(),
                    reason: unproven::JOINED,
                },
                "unexpected refusal for {sql:?}"
            );
        }

        // An inner join is refused by the same rule, for the narrower reason
        // stated in `schema_basis`: nothing nulls a column, but an output
        // name under a join resolves against more than one relation.
        let inner = plan_page(
            "SELECT a.ts AS ts, b.body AS b_body FROM logs AS a \
             INNER JOIN logs AS b ON a.ts = b.ts ORDER BY b_body, ts",
            None,
        )
        .expect_err("refused");
        assert_eq!(
            inner,
            PagePlanError::OrderTermNullabilityUnknown {
                column: "b_body".to_string(),
                reason: unproven::JOINED,
            },
        );
    }

    /// `ROLLUP`, `CUBE` and `GROUPING SETS` null a grouping column in their
    /// super-aggregate rows.
    ///
    /// Here the schema lookup is CORRECT -- `logs.severity_text` really is
    /// NOT NULL -- and the grouping construct introduces the NULL anyway. The
    /// prover modelled nothing about it, so the super-aggregate row was
    /// dropped from every page by the keyset predicate.
    #[test]
    fn refuses_a_grouping_column_under_rollup_cube_or_grouping_sets() {
        let groupings = [
            // The expression spelling.
            "ROLLUP(severity_text)",
            "CUBE(severity_text)",
            "GROUPING SETS ((severity_text), ())",
            // The modifier spelling of the same thing.
            "severity_text WITH ROLLUP",
            "severity_text WITH CUBE",
        ];
        for grouping in groupings {
            let sql = format!(
                "SELECT severity_text, count(*) AS c FROM logs \
                 GROUP BY {grouping} ORDER BY severity_text"
            );
            let err = plan_page(&sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::OrderTermNullabilityUnknown {
                    column: "severity_text".to_string(),
                    reason: unproven::GROUPING_NULLS,
                },
                "unexpected refusal for {sql:?}"
            );
        }

        // A plain GROUP BY adds no super-aggregate row, so it is not refused
        // by this rule: the narrowing is about the grouping construct, not
        // about grouping.
        let plain = plan_page(
            "SELECT severity_text, count(*) AS c FROM logs GROUP BY severity_text \
             ORDER BY severity_text",
            None,
        )
        .expect("planned");
        assert_eq!(
            plain.not_total,
            Some(NotTotalOrder::NoRowIdentity { table: "logs" }),
        );
    }

    /// Every order term is quoted, so a column aliased with a quoted keyword
    /// stays a column reference in the page statement.
    ///
    /// The term text is the one place caller-supplied text reaches the
    /// rewritten statement. Rendered bare, `select` re-parses as syntax rather
    /// than as the column: the ordering becomes a constant and the keyset
    /// predicate evaluates to NULL, so the page comes back empty with nothing
    /// to say why.
    #[test]
    fn quotes_every_order_term_so_a_keyword_alias_stays_a_column() {
        let plan = plan_page(
            "SELECT ts, body AS \"select\" FROM logs ORDER BY \"select\", ts",
            Some(&ResumePosition::new(vec![
                ResumeValue::Str("x".to_string()),
                ResumeValue::TimestampNanos(9),
            ])),
        )
        .expect("planned");
        assert_eq!(
            plan.order_by,
            vec![OrderTerm::ascending("select"), OrderTerm::ascending("ts")],
        );
        assert_eq!(
            plan.statement,
            "SELECT * FROM (SELECT ts, body AS \"select\" FROM logs) AS ravel_page \
             WHERE ((\"select\" > 'x') OR (\"select\" = 'x' \
             AND \"ts\" > arrow_cast(9, 'Timestamp(Nanosecond, None)'))) \
             ORDER BY \"select\" ASC, \"ts\" ASC",
        );
    }

    /// The one property of the wrap that text alone cannot settle: whether the
    /// derived-table alias changes how an inner statement with two output
    /// columns of the same name is planned. Answered against the real planner
    /// rather than by reasoning about it, on the `samples` schema registered
    /// as a plain in-memory table, because the question is DataFusion's own
    /// name resolution and the Ravel session does not touch it.
    ///
    /// It does not: the duplicate is rejected inside the inner projection,
    /// before an alias or an outer `ORDER BY` is reached, so both spellings
    /// fail with the identical message. That is what lets the planner emit
    /// such a statement without a check of its own -- the caller gets the same
    /// plan error for the page as for the statement it handed in, which is the
    /// property [`Projection::projects`] already relies on.
    #[tokio::test]
    async fn duplicate_output_names_fail_the_same_wrapped_as_at_top_level() {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(public_schema(), vec![vec![]]).expect("mem table");
        ctx.register_table(SAMPLES_TABLE, Arc::new(table))
            .expect("registered");

        let inner = "SELECT ts AS a, series_id AS a FROM samples";
        let top_level = ctx
            .state()
            .create_logical_plan(&format!("{inner} ORDER BY a"))
            .await
            .expect_err("duplicate output names are a plan error")
            .to_string();
        let wrapped = ctx
            .state()
            .create_logical_plan(&format!(
                "SELECT * FROM ({inner}) AS {PAGE_ALIAS} ORDER BY \"a\" ASC"
            ))
            .await
            .expect_err("duplicate output names are a plan error")
            .to_string();

        assert_eq!(top_level, wrapped);
        assert!(
            top_level.contains("Projections require unique expression names"),
            "unexpected plan error: {top_level}"
        );
    }

    /// A literal order term plans: no schema describes it and none needs to.
    ///
    /// The prover reached a defining COLUMN REFERENCE or nothing, so an alias
    /// over `1` refused with the same message as a term that can really be
    /// NULL. A literal is NON NULL in every row of every relation.
    #[test]
    fn a_literal_order_term_plans() {
        for sql in [
            "SELECT 1 AS a ORDER BY a",
            "SELECT 'x' AS a ORDER BY a",
            "SELECT -1 AS a ORDER BY a",
            "SELECT (1) AS a ORDER BY a",
            "SELECT 1 AS a, ts, series_id FROM samples ORDER BY a, ts, series_id",
        ] {
            let plan = plan_page(sql, None);
            assert!(plan.is_ok(), "refused {sql:?}: {plan:?}");
        }

        // A literal NULL and a placeholder are not literals this admits: the
        // first IS the hazard, and the second carries a value that only
        // arrives at execution time.
        for (sql, column) in [
            ("SELECT NULL AS a ORDER BY a", "a"),
            ("SELECT $1 AS a ORDER BY a", "a"),
        ] {
            let err = plan_page(sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::OrderTermNullabilityUnknown {
                    column: column.to_string(),
                    reason: unproven::EXPRESSION_NOT_PROVABLE,
                },
                "unexpected refusal for {sql:?}"
            );
        }
    }

    /// `count(...)` is NON NULL in every spelling: it returns 0, never NULL,
    /// over empty and over all-NULL input.
    ///
    /// Ordering on the bare call is still refused, one step earlier and for a
    /// different reason: every effective term has to be a projected output
    /// column so the wrap can reference it, and `PagePlanError::
    /// OrderTermNotColumn` is that rule. The alias is the spelling that
    /// reaches this prover.
    #[test]
    fn count_star_plans_through_an_alias() {
        for sql in [
            "SELECT count(*) AS c FROM logs GROUP BY severity_text ORDER BY c",
            "SELECT COUNT(*) AS c FROM logs GROUP BY severity_text ORDER BY c",
            "SELECT count(DISTINCT trace_id) AS c FROM logs GROUP BY severity_text ORDER BY c",
            "SELECT count(trace_id) AS c FROM logs GROUP BY severity_text ORDER BY c",
            // A grouping construct nulls a grouping COLUMN, never an
            // aggregate, so ordering on the count alone still plans.
            "SELECT count(*) AS c FROM logs GROUP BY ROLLUP(severity_text) ORDER BY c",
        ] {
            let plan = plan_page(sql, None);
            assert!(plan.is_ok(), "refused {sql:?}: {plan:?}");
        }

        let bare =
            plan_page("SELECT count(*) FROM logs ORDER BY count(*)", None).expect_err("refused");
        assert_eq!(
            bare,
            PagePlanError::OrderTermNotColumn {
                term: "count(*)".to_string(),
            },
        );
    }

    /// An alias resolves through to the expression that defines it, so the
    /// answer is about that expression and not about the alias being an
    /// alias.
    #[test]
    fn an_alias_resolves_through_to_its_defining_expression() {
        // Same alias, same statement shape, opposite answers: the defining
        // expression is the only thing that differs.
        let provable = plan_page(
            "SELECT count(*) AS ordering FROM logs GROUP BY severity_text ORDER BY ordering",
            None,
        );
        assert!(provable.is_ok(), "refused the count: {provable:?}");

        let not_provable = plan_page(
            "SELECT nullif(count(*), 0) AS ordering FROM logs GROUP BY severity_text \
             ORDER BY ordering",
            None,
        )
        .expect_err("refused");
        assert_eq!(
            not_provable,
            PagePlanError::OrderTermNullabilityUnknown {
                column: "ordering".to_string(),
                reason: unproven::EXPRESSION_NOT_PROVABLE,
            },
        );

        // Unary `+`/`-` and parentheses are NULL exactly when their operand
        // is, so the answer passes through them in both directions.
        let through = plan_page(
            "SELECT -count(*) AS ordering FROM logs GROUP BY severity_text ORDER BY ordering",
            None,
        );
        assert!(through.is_ok(), "refused the negated count: {through:?}");

        let through_null =
            plan_page("SELECT -(NULL) AS ordering ORDER BY ordering", None).expect_err("refused");
        assert_eq!(
            through_null,
            PagePlanError::OrderTermNullabilityUnknown {
                column: "ordering".to_string(),
                reason: unproven::EXPRESSION_NOT_PROVABLE,
            },
        );
    }

    /// `SUM`, `MIN`, `MAX` and `AVG` keep refusing, deliberately.
    ///
    /// Each returns NULL over empty input and over input that is entirely
    /// NULL, so an ordering on one can carry a row the keyset predicate drops
    /// from every page. Admitting them needs the grouped-versus-ungrouped
    /// analysis this prover does not do; until that exists the refusal is the
    /// correct answer and this test is what stops it being widened by
    /// analogy with `count`.
    #[test]
    fn sum_min_max_and_avg_still_refuse() {
        for call in [
            "sum(value)",
            "min(value)",
            "max(value)",
            "avg(value)",
            "SUM(value)",
        ] {
            let sql = format!(
                "SELECT {call} AS ordering FROM samples GROUP BY series_id ORDER BY ordering"
            );
            let err = plan_page(&sql, None).expect_err("refused");
            assert_eq!(
                err,
                PagePlanError::OrderTermNullabilityUnknown {
                    column: "ordering".to_string(),
                    reason: unproven::EXPRESSION_NOT_PROVABLE,
                },
                "unexpected refusal for {sql:?}"
            );
            assert!(
                err.to_string()
                    .contains("SUM, MIN, MAX and AVG are NULL over empty or all-NULL input"),
                "the message does not say why: {err}"
            );
        }
    }

    /// The ClickBench corpus, the largest body of real statements this
    /// repository holds, read from where the benchmarks keep it rather than
    /// copied, so a corpus edit shows up here as a failing count.
    const CLICKBENCH_CORPUS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/clickbench/hits.corpus.json"
    ));

    /// Whether a statement carries an `ORDER BY`, which is what makes it a
    /// candidate for paging at all.
    fn statement_has_an_order_by(sql: &str) -> bool {
        sql.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_uppercase()
            .contains("ORDER BY")
    }

    /// The trailing `LIMIT n [OFFSET m]` of a statement, removed.
    ///
    /// A refusal for a row limit is not a prover gap: a page IS a limit, so a
    /// statement carrying its own is refused by design and there is nothing
    /// to widen. Setting that clause aside is what makes the rest of the
    /// tally a measurement of the nullability prover.
    fn without_a_trailing_row_limit(sql: &str) -> String {
        let upper = sql.to_ascii_uppercase();
        let Some(at) = upper.rfind("LIMIT ") else {
            return sql.to_string();
        };
        let tail = sql[at + "LIMIT ".len()..].trim();
        let mut words = tail.split_whitespace();
        let digits = |word: &str| !word.is_empty() && word.bytes().all(|b| b.is_ascii_digit());
        let trailing = match (words.next(), words.next(), words.next(), words.next()) {
            (Some(rows), None, ..) => digits(rows),
            (Some(rows), Some(offset), Some(skipped), None) => {
                digits(rows) && offset.eq_ignore_ascii_case("OFFSET") && digits(skipped)
            }
            _ => false,
        };
        if trailing {
            sql[..at].trim_end().to_string()
        } else {
            sql.to_string()
        }
    }

    /// One statement's outcome, as the label the tally counts.
    ///
    /// A refusal is labelled by its variant, and an unproven-nullability
    /// refusal also by which link of the proof is missing: the two are
    /// different findings, and collapsing them would hide a prover gap
    /// widening into a real hazard or the reverse.
    fn page_plan_outcome(sql: &str) -> String {
        match plan_page(sql, None) {
            Ok(plan) if plan.total_order() => "plans, total order".to_string(),
            Ok(_) => "plans, not a total order".to_string(),
            Err(PagePlanError::Invalid(_)) => "Invalid".to_string(),
            Err(PagePlanError::CrossSignal) => "CrossSignal".to_string(),
            Err(PagePlanError::RowLimitInStatement) => "RowLimitInStatement".to_string(),
            Err(PagePlanError::PipeOperator) => "PipeOperator".to_string(),
            Err(PagePlanError::NoOrdering { .. }) => "NoOrdering".to_string(),
            Err(PagePlanError::OrderByAll) => "OrderByAll".to_string(),
            Err(PagePlanError::OrderTermNotColumn { .. }) => "OrderTermNotColumn".to_string(),
            Err(PagePlanError::OrderTermNotProjected { .. }) => "OrderTermNotProjected".to_string(),
            Err(PagePlanError::UnsupportedOrderOption { .. }) => {
                "UnsupportedOrderOption".to_string()
            }
            Err(PagePlanError::OrderTermNullable { .. }) => "OrderTermNullable".to_string(),
            Err(PagePlanError::OrderTermNullabilityUnknown { reason, .. }) => {
                format!("OrderTermNullabilityUnknown: {reason}")
            }
            Err(PagePlanError::ResumeArity { .. }) => "ResumeArity".to_string(),
            Err(PagePlanError::NonFiniteResumeValue) => "NonFiniteResumeValue".to_string(),
        }
    }

    fn tally(outcomes: &BTreeMap<String, usize>, prefix: &str) -> usize {
        outcomes
            .iter()
            .filter(|(label, _)| label.starts_with(prefix))
            .map(|(_, count)| *count)
            .sum()
    }

    fn expected(pairs: &[(&str, usize)]) -> BTreeMap<String, usize> {
        pairs
            .iter()
            .map(|(label, count)| ((*label).to_string(), *count))
            .collect()
    }

    /// What fraction of the ClickBench corpus this planner can page, pinned
    /// as exact counts.
    ///
    /// This is the figure the tool is judged on, so it is asserted rather
    /// than printed. Every number here is exact: a bare inequality would
    /// pass while the planner regressed to refusing everything, which is the
    /// direction that costs a caller a capability rather than correctness.
    ///
    /// Two passes. The first is the corpus as written, where 32 of the 43
    /// statements carry their own `LIMIT` and are refused for that alone.
    /// The second strips a trailing `LIMIT n [OFFSET m]`, which is what
    /// exposes the nullability prover underneath: 22 of the 43 plan, and 3
    /// of the 21 refusals are about nullability. The remaining 18 are about
    /// the ordering itself, not about NULLs -- no `ORDER BY` at all, a term
    /// that is not a column, a term the statement does not project -- and
    /// each is a separate piece of work.
    #[test]
    fn the_clickbench_corpus_page_plan_outcomes_are_pinned() {
        let corpus: serde_json::Value =
            serde_json::from_str(CLICKBENCH_CORPUS).expect("corpus parses");
        let entries = corpus["entries"].as_array().expect("corpus has entries");

        let mut statements = 0usize;
        let mut ordered = 0usize;
        let mut as_written: BTreeMap<String, usize> = BTreeMap::new();
        let mut without_row_limit: BTreeMap<String, usize> = BTreeMap::new();

        for entry in entries {
            let sql = entry["sql"].as_str().expect("entry has sql");
            statements += 1;
            if statement_has_an_order_by(sql) {
                ordered += 1;
            }
            *as_written.entry(page_plan_outcome(sql)).or_default() += 1;
            *without_row_limit
                .entry(page_plan_outcome(&without_a_trailing_row_limit(sql)))
                .or_default() += 1;
        }

        assert_eq!(statements, 43, "corpus statement count");
        assert_eq!(ordered, 32, "corpus statements carrying an ORDER BY");

        assert_eq!(
            as_written,
            expected(&[
                ("NoOrdering", 10),
                ("OrderTermNotColumn", 1),
                ("RowLimitInStatement", 32),
            ]),
            "outcomes for the corpus as written",
        );
        assert_eq!(tally(&as_written, "plans"), 0, "plans, as written");
        assert_eq!(
            statements - tally(&as_written, "plans"),
            43,
            "refusals, as written",
        );

        let mut want = expected(&[
            ("NoOrdering", 11),
            ("OrderTermNotColumn", 5),
            ("OrderTermNotProjected", 2),
            ("plans, not a total order", 22),
        ]);
        want.insert(
            format!(
                "OrderTermNullabilityUnknown: {}",
                unproven::EXPRESSION_NOT_PROVABLE
            ),
            2,
        );
        want.insert(
            format!(
                "OrderTermNullabilityUnknown: {}",
                unproven::NOT_A_SCHEMA_COLUMN
            ),
            1,
        );
        assert_eq!(
            without_row_limit, want,
            "outcomes once a trailing row limit is set aside",
        );
        assert_eq!(
            tally(&without_row_limit, "plans"),
            22,
            "plans, row limit set aside",
        );
        assert_eq!(
            statements - tally(&without_row_limit, "plans"),
            21,
            "refusals, row limit set aside",
        );
        assert_eq!(
            tally(&without_row_limit, "OrderTermNullab"),
            3,
            "nullability refusals, row limit set aside",
        );
    }

    /// A `samples` statement with no ORDER BY at all is pageable: the row
    /// identity is a complete ordering on its own, so the planner imposes it
    /// rather than refusing.
    #[test]
    fn imposes_the_row_identity_on_a_statement_with_no_ordering() {
        let plan = plan_page("SELECT * FROM samples", None).expect("planned");
        assert_eq!(
            plan.tiebreak_appended,
            vec!["ts".to_string(), "series_id".to_string()],
        );
        assert_eq!(
            plan.order_by,
            vec![
                OrderTerm::ascending("ts"),
                OrderTerm::ascending("series_id")
            ],
        );
        assert!(plan.total_order());
    }
}
