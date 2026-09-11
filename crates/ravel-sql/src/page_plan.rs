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
//! An `ORDER BY` term that can be NULL is refused as well
//! ([`PagePlanError::OrderTermNullable`]): a keyset comparison against NULL is
//! NULL, so the rows whose term is NULL match no disjunct and appear on no
//! page at all. That is a wrong answer on both the total and the not-total
//! path, because the keyset predicate is what resumes both. Only a term that
//! the text proves NON NULL is admitted: it has to be a bare reference to a
//! column the target table's public schema declares non-nullable, either
//! directly or through an alias over one. Everything else is refused,
//! including a projected expression (whose nullability no schema lookup
//! answers), a declared column (all nullable), a set-operation body (whose
//! projection is not readable from the text), an output name projected twice
//! under different sources, and a statement with no base table at all.
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

use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Distinct, Expr as SqlExpr, GroupByExpr, Ident, ObjectName, OrderBy, OrderByKind, Query,
    SelectItem, SetExpr, Statement, TableFactor,
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
    // body an incomplete description of the statement, so every check below it
    // would be answering about the wrong rows.
    if !query.pipe_operators.is_empty() {
        return Err(PagePlanError::PipeOperator);
    }
    if query.limit_clause.is_some() || query.fetch.is_some() {
        return Err(PagePlanError::RowLimitInStatement);
    }
    if let Some(SelectShape { top: Some(_), .. }) = shape_of(&query) {
        return Err(PagePlanError::RowLimitInStatement);
    }

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
    for term in &terms {
        if !term_is_non_nullable(target, &projection, &term.column) {
            return Err(PagePlanError::OrderTermNullable {
                column: term.column.clone(),
            });
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
/// for a bare column reference and for nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputSource {
    /// A bare reference to this column of the `FROM` relation, under its own
    /// name or an alias.
    Column(String),
    /// Anything else: a computed expression, or an output name the projection
    /// gives twice from different sources. Neither has a column in the
    /// target's schema to look up.
    Opaque,
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

    /// The target-table column the output column `column` is a bare reference
    /// to, or `None` when the text does not name one.
    ///
    /// A wildcard answers with the name itself: every output column of a
    /// `SELECT *` is a column of the `FROM` relation under its own name.
    /// `Unknown` answers `None` rather than guessing, which is what makes a
    /// set-operation body fail the nullability check instead of passing it
    /// unexamined.
    fn source_column<'a>(&'a self, column: &'a str) -> Option<&'a str> {
        match self {
            Projection::Wildcard => Some(column),
            Projection::Unknown => None,
            Projection::Columns(items) => match items.get(column) {
                Some(OutputSource::Column(name)) => Some(name.as_str()),
                Some(OutputSource::Opaque) | None => None,
            },
        }
    }
}

fn projection_of(query: &Query) -> Projection {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Projection::Unknown;
    };
    let mut names: BTreeMap<String, OutputSource> = BTreeMap::new();
    let mut record = |name: String, source: OutputSource| {
        // A name the projection gives twice is ambiguous here even when both
        // sources are columns, so it degrades to opaque rather than to
        // whichever item came last.
        let entry = names.entry(name).or_insert_with(|| source.clone());
        if *entry != source {
            *entry = OutputSource::Opaque;
        }
    };
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Projection::Wildcard;
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let source = column_of(expr).map_or(OutputSource::Opaque, OutputSource::Column);
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

/// Whether the text proves the effective term `column` is NON NULL.
///
/// The proof has to hold end to end: the output column has to be a bare
/// reference to a column of the target table (so a schema lookup is about the
/// right value at all), and that column has to be declared non-nullable in the
/// table's public schema. A statement with no base table has no schema to ask,
/// and a declared column is absent from the static schema and nullable
/// anyway, so both answer false.
fn term_is_non_nullable(target: PageTarget, projection: &Projection, column: &str) -> bool {
    let Some(table) = target.table_name() else {
        return false;
    };
    let Some(source) = projection.source_column(column) else {
        return false;
    };
    let schema = match table {
        SAMPLES_TABLE => public_schema(),
        LOGS_TABLE => logs_schema(),
        SPANS_TABLE => spans_schema(),
        ALERTS_TABLE => alerts_schema(),
        AUDIT_TABLE => audit_schema(),
        _ => return false,
    };
    schema
        .field_with_name(source)
        .is_ok_and(|field| !field.is_nullable())
}

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
            // declares nullable outright.
            (
                "SELECT nullif(value, 0) AS v, ts, series_id FROM samples ORDER BY v",
                PagePlanError::OrderTermNullable {
                    column: "v".to_string(),
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
    #[test]
    fn refuses_an_ordering_term_that_can_be_null() {
        let cases: Vec<(&str, &str)> = vec![
            // A projected expression: the case reachable on the TOTAL path.
            // This used to report `total_order() == true` while every keyset
            // disjunct was NULL for the rows `nullif` nulled out.
            (
                "SELECT nullif(value, 0) AS v, ts, series_id FROM samples ORDER BY v",
                "v",
            ),
            // Columns the schemas declare nullable, one per table that has
            // one.
            ("SELECT * FROM logs ORDER BY span_id, ts", "span_id"),
            ("SELECT * FROM spans ORDER BY service_name", "service_name"),
            ("SELECT * FROM alerts ORDER BY alert_id", "alert_id"),
            // An output name the projection gives twice from different
            // columns: both are columns, but which one the term means is not
            // readable from the text, so the nullable one cannot be ruled out.
            ("SELECT ts AS a, trace_id AS a FROM logs ORDER BY a", "a"),
            // A set-operation body carries no readable projection at all.
            (
                "SELECT ts FROM logs UNION ALL SELECT ts FROM logs ORDER BY ts",
                "ts",
            ),
            // No base table, so there is no schema to prove anything against.
            ("SELECT 1 AS a ORDER BY a", "a"),
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
