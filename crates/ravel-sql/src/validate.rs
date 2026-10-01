//! Security invariant 1: read-only single-statement SQL, and (ADR-2040 D2,
//! D4) the three admitted Parquet DDL statement forms.
//!
//! [`validate_query`] runs on the raw request text *before* any planning,
//! catalog resolution, or `SessionContext` construction. It accepts exactly
//! one statement and only when that statement is a read-only
//! `Statement::Query`. Everything else -- DDL in every form (including
//! DataFusion's own `CREATE EXTERNAL TABLE` extension), DML, `COPY`,
//! `SET`/`RESET`, transaction control, `EXPLAIN` (both the ANSI and the
//! DataFusion extension form), and a multi-statement body -- is rejected
//! with a typed error the endpoint maps to HTTP 400.
//!
//! [`validate_ddl`] is the sibling gate for the three statement forms D2
//! admits: `CREATE EXTERNAL TABLE`, `CREATE OR REPLACE EXTERNAL TABLE`, and
//! `DROP TABLE`, each over `PARQUET`. It never runs in the same request as
//! [`validate_query`]: the executor's `execute_ddl` entry point (D4) is the
//! only caller, and it is separate from `execute`/`execute_accounted`, which
//! stay read-only and keep calling [`validate_query`]. Like `validate_query`,
//! it is a pure text function with no session and no grants lookup: it
//! checks statement shape and `LOCATION` URL syntax, never whether a
//! `LOCATION` lies inside a grant (that needs the tenant's grants, and is
//! `execute_ddl`'s job, D2).
//!
//! Parsing goes through [`complexity_guard::parse_guarded`], the crate's only
//! parse of caller text, which runs the structural-complexity guard and then
//! DataFusion's own `DFParser` front end rather than bare sqlparser: it is the
//! front end `SessionContext::sql` uses, so a statement that parses into a
//! DataFusion extension variant (`CreateExternalTable`, `CopyTo`, `Explain`,
//! `Reset`) is seen here in the same shape the planner would see it. A gate
//! built on bare sqlparser would either fail to parse those or classify them
//! differently from the planner, which is exactly the kind of gap the
//! invariant exists to close.
//!
//! A `Query` is not automatically read-only in sqlparser's grammar: its body
//! is a `SetExpr`, which has `Insert`/`Update`/`Delete`/`Merge` variants
//! (`WITH ... INSERT ...` parses as `Statement::Query`). [`validate_query`]
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
//! `avg`/`mean` are admitted (ADR-0022 decisions 3, 4): they are
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

use crate::complexity_guard;
use datafusion::sql::parser::{CreateExternalTable, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart, ObjectType,
    Query, SetExpr, Statement, TableFactor, Value as SqlValue, Visit, Visitor,
};
use ravel_pqtable::grants::{self, GrantsError};
use ravel_pqtable::names;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

/// Every aggregate UDAF name (primary spelling and alias) the default
/// DataFusion session registers that is **not** in the admitted set
/// ([`ADMITTED_AGGREGATES`](crate::session::ADMITTED_AGGREGATES): `count`,
/// `sum`, `min`, `max`, `avg`, `mean`). ADR-0022 decision 2 makes exclusion the default: the
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
/// ADR-0022 decisions 3, 4).
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

    /// A scalar function excluded from the v1 subset appeared in the query.
    /// The admitted scalar surface is every default-registered deterministic
    /// scalar; the excluded ones ([`EXCLUDED_SCALARS`](crate::session::EXCLUDED_SCALARS))
    /// are nondeterministic or environment-reading (`uuid`, `random`, `now`,
    /// `version`, ...) and so cannot be attested by the differential
    /// conformance oracle (ADR-0097 decision 4). Without this variant such a
    /// call would surface only as an opaque plan failure once the registry
    /// gate refuses it; `name` is the offending lowercased spelling.
    #[error(
        "{name} is not part of the v1 SQL scalar function subset; it is \
         nondeterministic or environment-reading and is excluded, while the \
         admitted scalar surface is the deterministic string, unicode, \
         datetime, math, regex, and encoding functions \
         (docs/adrs/0097-sql-scalar-function-surface.md)"
    )]
    ExcludedScalar { name: String },

    /// A window function excluded from the v1 subset appeared with an `OVER`
    /// clause. The admitted window functions are the rank/offset families and
    /// the two correctly-rounded ratio functions; the excluded ones
    /// ([`EXCLUDED_WINDOWS`](crate::session::EXCLUDED_WINDOWS),
    /// `first_value`/`last_value`/`nth_value`) are refused pending a
    /// conformance-row decision (ADR-0097 decision 6). This variant makes that
    /// refusal window-aware: the message names the admitted window surface
    /// rather than claiming the call is an aggregate. `name` is the offending
    /// lowercased spelling.
    #[error(
        "{name} is not part of the v1 SQL window function subset; \
         the admitted window functions are row_number, rank, dense_rank, \
         ntile, lag, lead, cume_dist, percent_rank \
         (docs/adrs/0097-sql-scalar-function-surface.md)"
    )]
    ExcludedWindow { name: String },

    /// The statement text carries more structural tokens than
    /// [`MAX_STATEMENT_COMPLEXITY`](crate::complexity_guard::MAX_STATEMENT_COMPLEXITY),
    /// so it is refused before it is parsed (crate::complexity_guard, issue
    /// #1680). The message carries the two counts and nothing else of the
    /// caller's input, so it is safe to return verbatim like the other
    /// validation errors.
    #[error("{0}")]
    TooComplex(#[from] complexity_guard::StatementTooComplex),
}

impl From<complexity_guard::GuardedParseError> for ValidationError {
    fn from(error: complexity_guard::GuardedParseError) -> Self {
        match error {
            complexity_guard::GuardedParseError::TooComplex(too_complex) => {
                ValidationError::TooComplex(too_complex)
            }
            complexity_guard::GuardedParseError::Parse(message) => {
                ValidationError::Parse(strip_prefix(&message))
            }
        }
    }
}

/// Parse `sql` and accept it only if it is exactly one read-only
/// `Statement::Query` inside the v1 subset. Returns before any planning.
///
/// The structural-complexity guard runs first, before the text is parsed at
/// all: parsing a deep enough statement, and every recursive walk over the
/// tree it produces (the two below, `crate::page_plan`'s rewrites, DataFusion's
/// SQL-to-`LogicalPlan` conversion, and the tree's own `Drop`) recurses once
/// per tree level, and a stack overflow on a 2 MiB tokio worker stack aborts
/// the process rather than raising a catchable panic (issue #1680). That
/// ordering is not this function's to remember: it is what
/// [`complexity_guard::parse_guarded`] is, so every parse in the crate has it.
pub fn validate_query(sql: &str) -> Result<(), ValidationError> {
    let statements = complexity_guard::parse_guarded(sql)?;

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
    reject_excluded_functions(query)?;
    Ok(())
}

/// A typed intent for one of the three statement forms ADR-2040 D2 admits.
/// [`validate_ddl`] produces this from caller text; the executor's
/// `execute_ddl` is the only consumer, and it is what actually checks the
/// `location` against the tenant's grants, opens the external store, and
/// writes a manifest (D2, D4). Nothing here reads a grant, a store, or a
/// clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlIntent {
    /// `CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] name STORED AS
    /// PARQUET LOCATION '<url>' [OPTIONS (...)]`. `if_not_exists` and
    /// `or_replace` are never both `true`: DataFusion's own parser accepts
    /// at most one of `IF NOT EXISTS` and `OR REPLACE` in one statement.
    CreateExternal {
        name: String,
        if_not_exists: bool,
        or_replace: bool,
        location: String,
        options: BTreeMap<String, String>,
    },
    /// `DROP TABLE [IF EXISTS] name`.
    Drop { name: String, if_exists: bool },
}

/// A request rejected by the DDL gate, before any grant, store, or manifest
/// is touched.
///
/// No `Clone`/`PartialEq`/`Eq`: `GrantsError` (wrapped by [`Location`](
/// DdlValidationError::Location)) implements neither, since one of its own
/// variants wraps a `StoreError`. Tests match on the variant shape instead
/// of equality.
#[derive(Debug, thiserror::Error)]
pub enum DdlValidationError {
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

    /// The statement text carries more structural tokens than
    /// [`MAX_STATEMENT_COMPLEXITY`](crate::complexity_guard::MAX_STATEMENT_COMPLEXITY).
    #[error("{0}")]
    TooComplex(#[from] complexity_guard::StatementTooComplex),

    /// The statement parsed but is not one of the three admitted DDL forms
    /// (`CREATE EXTERNAL TABLE`, `CREATE OR REPLACE EXTERNAL TABLE`,
    /// `DROP TABLE`).
    #[error(
        "{kind} is not permitted on the DDL SQL endpoint; only CREATE EXTERNAL TABLE, \
         CREATE OR REPLACE EXTERNAL TABLE, and DROP TABLE are accepted (docs/adrs/\
         2040-parquet-tables-queried-in-place.md)"
    )]
    NotDdl { kind: &'static str },

    /// `TEMPORARY` is not part of the admitted subset (D2).
    #[error(
        "TEMPORARY is not supported for an external table (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    Temporary,

    /// `UNBOUNDED` is not part of the admitted subset (D2).
    #[error(
        "UNBOUNDED is not supported for an external table (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    Unbounded,

    /// `PARTITIONED BY` is not part of the admitted subset (D2).
    #[error(
        "PARTITIONED BY is not supported for an external table (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    PartitionedBy,

    /// `WITH ORDER` is not part of the admitted subset (D2).
    #[error(
        "WITH ORDER is not supported for an external table (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    WithOrder,

    /// A column list, or a table-level constraint, was given. The schema
    /// comes from the Parquet footer, never from caller-supplied columns
    /// (D1, D2).
    #[error(
        "a column list or table constraint is not supported; the schema is inferred \
         from the Parquet footer (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    ColumnList,

    /// `STORED AS <file_type>` named something other than `PARQUET`.
    #[error("STORED AS {file_type} is not supported; only STORED AS PARQUET is admitted")]
    NotParquet { file_type: String },

    /// An `OPTIONS` entry whose key is not `binary_as_string` or
    /// `ravel.cast.<column>` (D5).
    #[error(
        "OPTIONS key {key:?} is not admitted; only binary_as_string and ravel.cast.<column> \
         are supported (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    UnsupportedOption { key: String },

    /// An `OPTIONS` value that is not a plain quoted string literal.
    #[error("OPTIONS value for {key:?} must be a quoted string literal")]
    OptionValueNotString { key: String },

    /// An `OPTIONS` value admitted by key but not one of its allowed
    /// literals (D5): `binary_as_string` admits only `'true'`;
    /// `ravel.cast.<column>` admits only `'date-from-days'`,
    /// `'timestamp-from-seconds'`, or `'timestamp-from-millis'`.
    #[error(
        "OPTIONS value {value:?} for {key:?} is not admitted \
         (docs/adrs/2040-parquet-tables-queried-in-place.md)"
    )]
    InvalidOptionValue { key: String, value: String },

    /// The same `OPTIONS` key was given more than once.
    #[error("OPTIONS key {key:?} was given more than once")]
    DuplicateOption { key: String },

    /// The table name failed ravel-pqtable's name rule.
    #[error(transparent)]
    InvalidTableName(#[from] names::NameError),

    /// The `LOCATION` URL failed syntax validation: wrong scheme, a `..` or
    /// empty segment, a glob, a percent-escape, or a query string (D2, D4).
    /// `ravel_pqtable::grants::parse_location` is the sole source of this
    /// error; it never returns any other `GrantsError` variant.
    #[error(transparent)]
    Location(#[from] GrantsError),

    /// `DROP TABLE` named something other than exactly one table.
    #[error("DROP TABLE admits exactly one table name; the request named {count}")]
    DropMultipleTables { count: usize },

    /// `DROP` named an object type other than `TABLE`.
    #[error("only DROP TABLE is admitted; DROP {object_type} is not")]
    DropNotTable { object_type: String },

    /// `DROP TABLE` carried `CASCADE`, `RESTRICT`, `PURGE`, `TEMPORARY`, or
    /// the MySQL `ON <table>` drop-index form, none of which this subset
    /// supports.
    #[error("DROP TABLE does not support {clause}")]
    DropUnsupported { clause: &'static str },
}

impl From<complexity_guard::GuardedParseError> for DdlValidationError {
    fn from(error: complexity_guard::GuardedParseError) -> Self {
        match error {
            complexity_guard::GuardedParseError::TooComplex(too_complex) => {
                DdlValidationError::TooComplex(too_complex)
            }
            complexity_guard::GuardedParseError::Parse(message) => {
                DdlValidationError::Parse(strip_prefix(&message))
            }
        }
    }
}

/// Longest `ravel.cast.<column>` column name [`is_admitted_cast_column`]
/// admits, matching [`names::MAX_TABLE_NAME_LEN`] so a cast key's rendered
/// form stays bounded the same way a table name's does.
const MAX_CAST_COLUMN_LEN: usize = names::MAX_TABLE_NAME_LEN;

/// Whether `column` is an admitted `ravel.cast.<column>` column name: ASCII
/// letters of either case, digits, and `_`, first character a letter or `_`,
/// up to [`MAX_CAST_COLUMN_LEN`] bytes, with no quote or other punctuation.
/// D5 names no charset for a cast column, so this is deliberately not
/// [`names::validate_table`]'s table-name rule (lowercase only, reserved
/// names): that rule refuses D5's own example, `ravel.cast.EventDate`. The
/// column lookup [`crate::ddl::execute_ddl`] runs at execute time against the
/// snapshotted schema stays case-sensitive, since a Parquet column name is
/// case-sensitive; this rule exists only to keep the audit text this key
/// renders into ([`crate::redact::redact`]) unambiguous, the same reason
/// [`is_admitted_option_key`] exists at all.
fn is_admitted_cast_column(column: &str) -> bool {
    if column.is_empty() || column.len() > MAX_CAST_COLUMN_LEN {
        return false;
    }
    let mut chars = column.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Admitted `OPTIONS` key: exactly `binary_as_string`, or `ravel.cast.`
/// followed by a column name [`is_admitted_cast_column`] admits. Keeping that
/// rule narrow is what keeps a key like `ravel.cast.x'); DROP TABLE y--` out
/// of both [`validate_ddl`] and [`crate::redact::redact`] (they share this
/// function through [`create_external_intent`]): unchecked, that key reached
/// `render_create_external` and was written inside single quotes with no
/// escaping of its own.
fn is_admitted_option_key(key: &str) -> bool {
    key == "binary_as_string"
        || key
            .strip_prefix("ravel.cast.")
            .is_some_and(is_admitted_cast_column)
}

/// Whether `value` is one of the literals D5 admits for `key`. Only called
/// once `is_admitted_option_key(key)` is already true, so `key` is either
/// exactly `binary_as_string` or a `ravel.cast.` key with an admitted column.
fn is_admitted_option_value(key: &str, value: &str) -> bool {
    if key == "binary_as_string" {
        value == "true"
    } else {
        matches!(
            value,
            "date-from-days" | "timestamp-from-seconds" | "timestamp-from-millis"
        )
    }
}

/// Parse `sql` and accept it only if it is exactly one of the three D2
/// statement forms, returning the typed intent `execute_ddl` executes.
/// Like [`validate_query`], this returns before any planning, and it checks
/// no grant: `location`'s syntax is validated here (scheme, no `..`, no
/// glob, no percent-escape, no query string), but whether it lies inside a
/// grant is `execute_ddl`'s job, since that needs the tenant's grants (D2).
pub fn validate_ddl(sql: &str) -> Result<DdlIntent, DdlValidationError> {
    let statements = complexity_guard::parse_guarded(sql)?;

    if statements.len() > 1 {
        return Err(DdlValidationError::MultipleStatements {
            count: statements.len(),
        });
    }
    let statement = statements.front().ok_or(DdlValidationError::Empty)?;

    match statement {
        DFStatement::CreateExternalTable(create) => create_external_intent(create),
        DFStatement::Statement(inner) => match inner.as_ref() {
            Statement::Drop {
                object_type,
                if_exists,
                names: drop_names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } => drop_intent(
                *object_type,
                *if_exists,
                drop_names,
                *cascade,
                *restrict,
                *purge,
                *temporary,
                table.as_ref(),
            ),
            other => Err(DdlValidationError::NotDdl {
                kind: ansi_statement_kind(other),
            }),
        },
        DFStatement::CopyTo(_) => Err(DdlValidationError::NotDdl { kind: "COPY" }),
        DFStatement::Explain(_) => Err(DdlValidationError::NotDdl { kind: "EXPLAIN" }),
        DFStatement::Reset(_) => Err(DdlValidationError::NotDdl { kind: "RESET" }),
    }
}

/// `pub(crate)` beyond this module so [`crate::redact::redact`] can classify
/// and render the one admitted `CREATE EXTERNAL TABLE` shape with the exact
/// same rules [`validate_ddl`] enforces, rather than a second copy of them.
pub(crate) fn create_external_intent(
    create: &CreateExternalTable,
) -> Result<DdlIntent, DdlValidationError> {
    // Every field named, no `..`: a field a future datafusion-sql adds to
    // this struct must fail this match until it is given an admitted-or-
    // refused decision here, rather than silently riding along unexamined.
    let CreateExternalTable {
        name,
        columns,
        file_type,
        location,
        table_partition_cols,
        order_exprs,
        if_not_exists,
        or_replace,
        temporary,
        unbounded,
        options: raw_options,
        constraints,
    } = create;

    if *temporary {
        return Err(DdlValidationError::Temporary);
    }
    if *unbounded {
        return Err(DdlValidationError::Unbounded);
    }
    if !table_partition_cols.is_empty() {
        return Err(DdlValidationError::PartitionedBy);
    }
    if !order_exprs.is_empty() {
        return Err(DdlValidationError::WithOrder);
    }
    if !columns.is_empty() || !constraints.is_empty() {
        return Err(DdlValidationError::ColumnList);
    }
    if file_type != "PARQUET" {
        return Err(DdlValidationError::NotParquet {
            file_type: file_type.clone(),
        });
    }

    let mut options = BTreeMap::new();
    for (key, value) in raw_options {
        if !is_admitted_option_key(key) {
            return Err(DdlValidationError::UnsupportedOption { key: key.clone() });
        }
        let SqlValue::SingleQuotedString(value) = value else {
            return Err(DdlValidationError::OptionValueNotString { key: key.clone() });
        };
        if !is_admitted_option_value(key, value) {
            return Err(DdlValidationError::InvalidOptionValue {
                key: key.clone(),
                value: value.clone(),
            });
        }
        if options.insert(key.clone(), value.clone()).is_some() {
            return Err(DdlValidationError::DuplicateOption { key: key.clone() });
        }
    }

    // Syntax only: whether `location` lies inside a grant is execute_ddl's
    // job (D2), which is the only caller that has a tenant's grants to check
    // it against.
    grants::parse_location(location)?;

    let name = name.to_string();
    names::validate_table(&name)?;

    Ok(DdlIntent::CreateExternal {
        name,
        if_not_exists: *if_not_exists,
        or_replace: *or_replace,
        location: location.clone(),
        options,
    })
}

#[allow(clippy::too_many_arguments)]
fn drop_intent(
    object_type: ObjectType,
    if_exists: bool,
    drop_names: &[datafusion::sql::sqlparser::ast::ObjectName],
    cascade: bool,
    restrict: bool,
    purge: bool,
    temporary: bool,
    table: Option<&datafusion::sql::sqlparser::ast::ObjectName>,
) -> Result<DdlIntent, DdlValidationError> {
    if object_type != ObjectType::Table {
        return Err(DdlValidationError::DropNotTable {
            object_type: object_type.to_string(),
        });
    }
    if cascade {
        return Err(DdlValidationError::DropUnsupported { clause: "CASCADE" });
    }
    if restrict {
        return Err(DdlValidationError::DropUnsupported { clause: "RESTRICT" });
    }
    if purge {
        return Err(DdlValidationError::DropUnsupported { clause: "PURGE" });
    }
    if temporary {
        return Err(DdlValidationError::DropUnsupported {
            clause: "TEMPORARY",
        });
    }
    if table.is_some() {
        return Err(DdlValidationError::DropUnsupported {
            clause: "the MySQL DROP INDEX ON <table> form",
        });
    }
    if drop_names.len() != 1 {
        return Err(DdlValidationError::DropMultipleTables {
            count: drop_names.len(),
        });
    }
    let name = drop_names[0].to_string();
    names::validate_table(&name)?;
    Ok(DdlIntent::Drop { name, if_exists })
}

/// The base table names `sql` references, lowercased and unqualified (the
/// last identifier of any multi-part name). Parsing reuses the same
/// `DFParser` front end as [`validate_query`], so the extraction is as robust
/// against comments, string literals, and quoting as the planner itself --
/// never a raw-text scan of the statement.
///
/// The executor uses this to pick which signal a query needs before planning
/// (ADR-0033: `samples` -> `Signal::Metrics`, `logs` -> `Signal::Logs`),
/// resolving a `Signal::Logs` snapshot only when `logs` is referenced and
/// rejecting a query that references both. The walk descends the whole query
/// tree (CTEs, set operations, subqueries, derived tables), so a `logs`
/// reference nested anywhere is seen.
///
/// A `WITH <name> AS (...)` common table expression declares a query-local
/// name that is *not* a base table: `WITH logs AS (SELECT value FROM samples)
/// SELECT count(*) FROM logs` never reads the real `logs` RLOG table, and must
/// resolve to metrics only. So the collected set has every CTE-declared name
/// subtracted from it. This uses the cheap, conservative whole-tree
/// approximation ADR-0033's amendment sanctions: collect every CTE alias
/// declared anywhere in the tree and subtract them all, rather than doing
/// per-scope shadow resolution. sqlparser's AST exposes no ready per-scope
/// resolver, and this crate's other pruning/extraction logic is already
/// widen-only (it never narrows incorrectly); a base table genuinely named
/// the same as a sibling-scope CTE is not a shape SQL v1 produces, and the
/// only cost of the approximation is failing to treat such a collision as a
/// real reference -- consistent with the rest of ravel-sql, never a
/// correctness hazard for the both-tables rejection, which still fires for
/// any query that reads both real tables (no CTE shadows a name it also
/// reads as a base table without the query being nonsensical).
pub(crate) fn referenced_base_tables(sql: &str) -> Result<BTreeSet<String>, ValidationError> {
    // This parses and walks a tree of its own, so it carries the same guard
    // [`validate_query`] does rather than relying on every caller having run
    // `validate` on the same text first. The scan stops one token past the
    // bound, so a statement that already passed `validate` pays a bounded
    // rescan and nothing else.
    let statements = complexity_guard::parse_guarded(sql)?;
    let mut tables = BTreeSet::new();
    let mut ctes = BTreeSet::new();
    for statement in &statements {
        if let DFStatement::Statement(inner) = statement
            && let Statement::Query(query) = inner.as_ref()
        {
            let _ = query.visit(&mut TableNameCollector {
                tables: &mut tables,
                ctes: &mut ctes,
            });
        }
    }
    // A CTE-declared name is query-local, never a base-table reference.
    tables.retain(|table| !ctes.contains(table));
    Ok(tables)
}

/// The first table reference in `sql` that can never name a table a session
/// registers: a table-function call (`read_parquet(...)`, `TABLE(...)`,
/// `range(0, 10)`), or a table name that is a URL or a path, which is any name
/// part quoted with `'` or holding a `/` or a `:` (`'s3://bucket/x.parquet'`,
/// `'ravel-pq://<tenant_hash>/...'`).
///
/// A session admits no table function and has no URL table, so such a
/// statement always fails to plan. The executor refuses it with the same
/// planning error before resolving anything, so it reads no object from any
/// store (ADR-2040 decision D4).
///
/// It must run before `resolve_parquet_target`'s `others.is_empty()`
/// short-circuit, so a URL table is refused whether or not a Parquet name is
/// present.
pub(crate) fn unreadable_table_reference(sql: &str) -> Result<Option<String>, ValidationError> {
    let statements = complexity_guard::parse_guarded(sql)?;
    let mut found = None;
    for statement in &statements {
        if let DFStatement::Statement(inner) = statement
            && let Statement::Query(query) = inner.as_ref()
            && let ControlFlow::Break(name) = query.visit(&mut UnreadableTableFinder)
        {
            found = Some(name);
            break;
        }
    }
    Ok(found)
}

/// Stops at the first table factor [`unreadable_table_reference`] describes.
struct UnreadableTableFinder;

impl Visitor for UnreadableTableFinder {
    type Break = String;

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<String> {
        match factor {
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name, .. } => ControlFlow::Break(name.to_string()),
            TableFactor::TableFunction { expr, .. } => ControlFlow::Break(format!("TABLE({expr})")),
            TableFactor::Table { name, .. } => {
                let url_like = name.0.iter().any(|part| match part {
                    ObjectNamePart::Identifier(ident) => {
                        ident.quote_style == Some('\'')
                            || ident.value.contains('/')
                            || ident.value.contains(':')
                    }
                    ObjectNamePart::Function(_) => true,
                });
                if url_like {
                    ControlFlow::Break(name.to_string())
                } else {
                    ControlFlow::Continue(())
                }
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

/// Collects every table-factor name in a query tree, plus every CTE alias the
/// tree declares, both lowercased and reduced to their bare (unqualified)
/// identifier. Never breaks: it visits the whole tree. The caller subtracts
/// the CTE names from the table names so a CTE named `logs`/`samples` is not
/// mistaken for the real table of that name.
struct TableNameCollector<'a> {
    tables: &'a mut BTreeSet<String>,
    ctes: &'a mut BTreeSet<String>,
}

impl Visitor for TableNameCollector<'_> {
    type Break = ();

    /// Record every CTE name this query's `WITH` clause declares. A
    /// `TableFactor::Table` reference to one of these names is resolved by
    /// the CTE, not by a base table, so the caller excludes them.
    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.ctes.insert(cte.alias.name.value.to_ascii_lowercase());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, .. } = factor {
            // `ObjectName`'s `Display` joins parts with '.'; take the last
            // segment and strip any identifier quoting so `public."samples"`
            // and `samples` both reduce to `samples`.
            let full = name.to_string();
            let bare = full.rsplit('.').next().unwrap_or(&full).trim_matches('"');
            self.tables.insert(bare.to_ascii_lowercase());
        }
        ControlFlow::Continue(())
    }
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
/// arms named explicitly are the ones called out; everything else
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
        Statement::Query(_) => "a SELECT statement",
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

/// Whether `bare` names a scalar function excluded from the v1 subset. Sourced
/// straight from [`EXCLUDED_SCALARS`](crate::session::EXCLUDED_SCALARS) so this
/// message layer cannot drift from the registry allowlist
/// `crate::session::build_session` enforces: a name added to that constant is
/// picked up here with no second edit (ADR-0097 decision 9).
fn is_excluded_scalar(bare: &str) -> bool {
    crate::session::EXCLUDED_SCALARS.contains(&bare)
}

/// Whether `bare` names a window function excluded from the v1 subset. Sourced
/// straight from [`EXCLUDED_WINDOWS`](crate::session::EXCLUDED_WINDOWS), the
/// same anti-drift discipline [`is_excluded_scalar`] uses.
fn is_excluded_window(bare: &str) -> bool {
    crate::session::EXCLUDED_WINDOWS.contains(&bare)
}

/// Classify a function call by its bare (lowercased, unqualified) name into the
/// typed error naming why it is refused, or `None` if the name is admitted.
///
/// `windowed` is whether the call carried an `OVER` clause. It decides the one
/// ambiguous case: `first_value`/`last_value`/`nth_value` are in **both** the
/// excluded-aggregate and excluded-window lists, because DataFusion registers
/// them in both registries. Used with `OVER` the call resolves through the
/// window registry and deserves the window-aware message; used bare it is an
/// aggregate and keeps the aggregate message the existing tests pin. Aggregate
/// therefore takes precedence over a bare window-name match, and a windowed
/// excluded-window name is caught first.
fn classify_excluded(bare: &str, windowed: bool) -> Option<ValidationError> {
    if windowed && is_excluded_window(bare) {
        return Some(ValidationError::ExcludedWindow {
            name: bare.to_string(),
        });
    }
    if is_excluded_aggregate(bare) {
        return Some(ValidationError::ExcludedAggregate {
            name: bare.to_string(),
        });
    }
    if is_excluded_scalar(bare) {
        return Some(ValidationError::ExcludedScalar {
            name: bare.to_string(),
        });
    }
    // A window-only excluded name used without `OVER` (none exists today, since
    // all three excluded windows are also excluded aggregates caught above);
    // kept so a future window-only addition to EXCLUDED_WINDOWS is still named.
    if is_excluded_window(bare) {
        return Some(ValidationError::ExcludedWindow {
            name: bare.to_string(),
        });
    }
    None
}

/// Reject excluded functions written as bare or schema-qualified names,
/// anywhere in the query, including inside subqueries and nested function
/// arguments. This is the message layer over the registry allowlists
/// `crate::session::build_session` enforces (ADR-0097 rejected alternative C):
/// the allowlist is what fails closed, this walk runs first and turns the
/// refusal into a typed error naming the admitted surface.
///
/// One spelling is not covered: a quoted identifier such as `"uuid"()` keeps
/// its quote characters in `func.name.to_string()`, so the exact-equality
/// match misses it. That query is still refused, by the registry gate, but
/// with the opaque plan error this walk exists to replace. Because the walk
/// is only a message layer, a miss costs error quality and never admits
/// anything.
/// It admits nothing on its own -- a name absent here is still refused by the
/// gate, only with a worse message -- and it sources its excluded scalar/window
/// names from the same constants the gate uses, so the two cannot diverge.
fn reject_excluded_functions(query: &Query) -> Result<(), ValidationError> {
    if let ControlFlow::Break(err) = query.visit(&mut ExcludedFunctionFinder) {
        return Err(err);
    }
    Ok(())
}

struct ExcludedFunctionFinder;

impl Visitor for ExcludedFunctionFinder {
    /// The typed rejection, carried out so the caller returns it verbatim.
    type Break = ValidationError;

    fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<ValidationError> {
        if let SqlExpr::Function(func) = expr {
            let name = func.name.to_string().to_ascii_lowercase();
            // Match the bare name and any schema-qualified spelling
            // (`public.uuid`), which the planner resolves to the same UDF.
            let bare = name.rsplit('.').next().unwrap_or(name.as_str());
            if let Some(err) = classify_excluded(bare, func.over.is_some()) {
                return ControlFlow::Break(err);
            }
            // sqlparser does not descend into `FunctionArguments::List`
            // expressions from `pre_visit_expr` on the enclosing call in
            // every version; walk them explicitly so `abs(uuid())`-shaped
            // nesting cannot hide an excluded function.
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
    use ravel_pqtable::grants::LocationDefect;

    fn reject(sql: &str) -> ValidationError {
        validate_query(sql).expect_err("must be rejected")
    }

    /// Table functions and URL-shaped names are found wherever they sit in
    /// the statement; a plain, qualified or quoted identifier is not one.
    #[test]
    fn unreadable_table_references_are_found_and_plain_names_are_not() {
        for sql in [
            "SELECT * FROM 's3://b/x.parquet'",
            "SELECT * FROM \"file:///etc/passwd\"",
            "SELECT * FROM read_parquet('x')",
            "SELECT * FROM TABLE(read_parquet('x'))",
            "SELECT * FROM hits WHERE id IN (SELECT value FROM range(0, 3))",
            "WITH t AS (SELECT * FROM generate_series(0, 3)) SELECT * FROM t",
        ] {
            assert!(
                unreadable_table_reference(sql).expect("parses").is_some(),
                "{sql}"
            );
        }
        for sql in [
            "SELECT * FROM hits",
            "SELECT * FROM public.hits",
            "SELECT * FROM \"Hits\" JOIN logs ON true",
            "WITH t AS (SELECT 1) SELECT * FROM t",
            "SELECT 1",
        ] {
            assert_eq!(
                unreadable_table_reference(sql).expect("parses"),
                None,
                "{sql}"
            );
        }
    }

    #[test]
    fn plain_select_is_accepted() {
        validate_query("SELECT ts, value FROM samples WHERE ts > 0 ORDER BY ts LIMIT 10")
            .expect("read-only select");
    }

    #[test]
    fn aggregates_in_the_v1_subset_are_accepted() {
        validate_query(
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
            validate_query(sql).unwrap_or_else(|e| panic!("min/max must be accepted: {sql}: {e}"));
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
        let err = validate_query("WITH c AS (SELECT 1) INSERT INTO samples VALUES (1, 2.0)");
        match err {
            // Either shape is a correct rejection: some dialect versions
            // parse this as a top-level INSERT, others as a Query whose body
            // is SetExpr::Insert. Both must be refused.
            Err(ValidationError::WriteInQuery { kind: "INSERT" })
            | Err(ValidationError::NotReadOnly { kind: "INSERT" }) => {}
            other => panic!("WITH ... INSERT must be rejected, got {other:?}"),
        }
    }

    /// `avg`/`mean` are admitted (ADR-0022 decisions 3, 4): the
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
            validate_query(sql).unwrap_or_else(|e| panic!("avg/mean must be accepted: {sql}: {e}"));
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

    /// An excluded nondeterministic scalar (`uuid`) is rejected before
    /// planning with the scalar-specific variant, not the aggregate one: an
    /// excluded scalar is not an aggregate and the message must not claim it
    /// is (ADR-0097 decision 9).
    #[test]
    fn excluded_scalar_is_rejected_with_the_scalar_variant() {
        assert_eq!(
            reject("SELECT uuid() FROM logs"),
            ValidationError::ExcludedScalar {
                name: "uuid".to_string()
            }
        );
    }

    /// An excluded window function used with `OVER` is rejected with the
    /// window-specific variant, whose message names the admitted window
    /// surface rather than claiming the call is an aggregate -- even though
    /// `first_value` is also an excluded aggregate name (ADR-0097 decision 9).
    #[test]
    fn excluded_window_over_is_rejected_with_the_window_variant() {
        assert_eq!(
            reject("SELECT first_value(body) OVER (ORDER BY ts) FROM logs"),
            ValidationError::ExcludedWindow {
                name: "first_value".to_string()
            }
        );
        let msg = ValidationError::ExcludedWindow {
            name: "first_value".to_string(),
        }
        .to_string();
        for admitted in crate::session::ADMITTED_WINDOWS {
            assert!(
                msg.contains(admitted),
                "window message must name the admitted window function {admitted}: {msg}"
            );
        }
    }

    /// The same excluded window name used *without* `OVER` resolves through the
    /// aggregate registry, so it keeps the aggregate message the existing tests
    /// pin: the `OVER` clause is what selects the window-aware error.
    #[test]
    fn excluded_window_name_without_over_stays_an_aggregate() {
        assert_eq!(
            reject("SELECT first_value(value) FROM samples"),
            ValidationError::ExcludedAggregate {
                name: "first_value".to_string()
            }
        );
    }

    /// Deliverable 2, matching discipline for the new variants: the scalar walk
    /// is case-insensitive, strips schema qualification (`public.uuid`), and
    /// descends into nested function arguments (`abs(uuid())`), exactly as the
    /// aggregate walk does. One property per case.
    #[test]
    fn excluded_scalar_matching_is_case_insensitive() {
        assert_eq!(
            reject("SELECT UUID() FROM logs"),
            ValidationError::ExcludedScalar {
                name: "uuid".to_string()
            }
        );
    }

    #[test]
    fn excluded_scalar_matching_strips_schema_qualification() {
        assert_eq!(
            reject("SELECT public.uuid() FROM logs"),
            ValidationError::ExcludedScalar {
                name: "uuid".to_string()
            }
        );
    }

    #[test]
    fn excluded_scalar_nested_in_a_call_cannot_hide() {
        assert_eq!(
            reject("SELECT abs(uuid()) FROM logs"),
            ValidationError::ExcludedScalar {
                name: "uuid".to_string()
            }
        );
    }

    /// The walk cannot admit: its excluded-name predicates are sourced directly
    /// from the [`EXCLUDED_SCALARS`](crate::session::EXCLUDED_SCALARS) and
    /// [`EXCLUDED_WINDOWS`](crate::session::EXCLUDED_WINDOWS) constants
    /// `build_session` enforces, so the two sets are identical by construction
    /// and cannot drift. A name cannot be dropped from the walk without dropping
    /// it from the constant, and a name absent from the walk is still refused by
    /// the registry gate (proven independently by
    /// `session::tests::admitted_and_excluded_cover_all_registries_for_every_table`,
    /// which asserts `build_session` removes every excluded name from its
    /// registry). This walk only ever produces a *better message*; it is never
    /// the thing that admits or refuses.
    #[test]
    fn the_walk_sources_exactly_the_excluded_constants() {
        for name in crate::session::EXCLUDED_SCALARS {
            assert!(
                is_excluded_scalar(name),
                "{name} is in EXCLUDED_SCALARS but the walk does not treat it as excluded"
            );
        }
        for name in crate::session::EXCLUDED_WINDOWS {
            assert!(
                is_excluded_window(name),
                "{name} is in EXCLUDED_WINDOWS but the walk does not treat it as excluded"
            );
        }
        // Admitted functions are not swept up by the excluded predicates.
        assert!(!is_excluded_scalar("lower"));
        assert!(!is_excluded_scalar("date_trunc"));
        assert!(!is_excluded_window("row_number"));
        assert!(!is_excluded_window("rank"));
    }

    /// A representative spread of the excluded nondeterministic/environment
    /// scalars is refused, not just `uuid`.
    #[test]
    fn nondeterministic_scalars_are_excluded() {
        for func in ["random", "rand", "now", "version"] {
            assert_eq!(
                reject(&format!("SELECT {func}() FROM logs")),
                ValidationError::ExcludedScalar {
                    name: func.to_string()
                },
                "{func} must be excluded from the v1 scalar subset"
            );
        }
    }

    #[test]
    fn empty_and_unparsable_bodies_are_rejected_without_planning() {
        assert_eq!(validate_query(""), Err(ValidationError::Empty));
        assert!(matches!(
            validate_query("SELECT ((( FROM samples"),
            Err(ValidationError::Parse(_))
        ));
    }

    fn tables(sql: &str) -> BTreeSet<String> {
        referenced_base_tables(sql).expect("parses")
    }

    #[test]
    fn referenced_tables_are_extracted_via_the_parser_not_raw_text() {
        // A plain FROM.
        assert!(tables("SELECT ts FROM logs").contains("logs"));
        assert!(!tables("SELECT ts FROM logs").contains("samples"));

        // A string literal that mentions the other table must NOT count: the
        // whole point of parsing rather than substring-matching.
        let only_samples = tables("SELECT body FROM samples WHERE body = 'from logs table'");
        assert!(only_samples.contains("samples"));
        assert!(
            !only_samples.contains("logs"),
            "a string literal is not a table reference: {only_samples:?}"
        );

        // A comment mentioning the other table must not count either.
        let commented = tables("SELECT ts FROM logs -- not from samples\n");
        assert!(commented.contains("logs") && !commented.contains("samples"));

        // Quoting and schema qualification reduce to the bare lowercased name.
        assert!(tables("SELECT ts FROM \"logs\"").contains("logs"));
    }

    #[test]
    fn referenced_tables_see_nested_and_both_table_references() {
        // A subquery reference is found.
        let nested = tables("SELECT ts FROM (SELECT ts FROM logs) t");
        assert!(nested.contains("logs"));

        // A query touching both tables surfaces both names (the executor turns
        // this into a rejection; here we only prove the extractor sees both).
        let both = tables("SELECT * FROM samples JOIN logs ON samples.ts = logs.ts");
        assert!(both.contains("samples") && both.contains("logs"));

        // A constant query references neither.
        assert!(tables("SELECT 1").is_empty());
    }

    /// The two-real-table detection ADR-0033 and ADR-0045 decision 5 both
    /// rely on (`target_signal`'s `has_samples`/`has_logs` check is the same
    /// idea for its own pair) must see every one of the three real-table
    /// pairs `samples`/`logs`/`spans` can form, since `referenced_base_tables`
    /// is signal-agnostic and does not special-case which table names are
    /// "real": adding `spans` as a third table needed no change here, only a
    /// query proving it.
    #[test]
    fn referenced_tables_see_all_three_real_table_pairs() {
        let samples_spans =
            tables("SELECT * FROM samples JOIN spans ON samples.ts = spans.start_ts");
        assert!(samples_spans.contains("samples") && samples_spans.contains("spans"));

        let logs_spans = tables("SELECT * FROM logs JOIN spans ON logs.ts = spans.start_ts");
        assert!(logs_spans.contains("logs") && logs_spans.contains("spans"));
    }

    /// A CTE whose declared name collides with a real table name is not a
    /// reference to that real table: `WITH logs AS (...) ... FROM logs` reads
    /// only whatever the CTE body reads. Regression for the ADR-0033 wiring,
    /// which collected every `TableFactor::Table` name indiscriminately and so
    /// rejected this legal metrics-only query as cross-signal.
    #[test]
    fn a_cte_named_like_a_table_is_not_that_base_table() {
        // A CTE named `logs` reading only `samples` resolves to samples alone.
        let via_cte = tables("WITH logs AS (SELECT value FROM samples) SELECT count(*) FROM logs");
        assert!(
            via_cte.contains("samples"),
            "the CTE body's real base table is still seen: {via_cte:?}"
        );
        assert!(
            !via_cte.contains("logs"),
            "a CTE named `logs` is not the real logs table: {via_cte:?}"
        );

        // Symmetric case: a CTE named `samples` reading only `logs`.
        let via_cte =
            tables("WITH samples AS (SELECT body FROM logs) SELECT count(*) FROM samples");
        assert!(
            via_cte.contains("logs"),
            "the CTE body's real base table is still seen: {via_cte:?}"
        );
        assert!(
            !via_cte.contains("samples"),
            "a CTE named `samples` is not the real samples table: {via_cte:?}"
        );
    }

    /// A CTE named `spans` is query-local, just like the existing `logs`/
    /// `samples` cases: it must not be mistaken for the real `spans` table
    /// (ADR-0045 decision 5).
    #[test]
    fn a_cte_named_spans_is_not_that_base_table() {
        let via_cte =
            tables("WITH spans AS (SELECT value FROM samples) SELECT count(*) FROM spans");
        assert!(
            via_cte.contains("samples"),
            "the CTE body's real base table is still seen: {via_cte:?}"
        );
        assert!(
            !via_cte.contains("spans"),
            "a CTE named `spans` is not the real spans table: {via_cte:?}"
        );
    }

    /// The CTE exclusion is whole-tree: a CTE declared in a subquery shadows
    /// its name for the whole extraction too, and a real base table still
    /// surfaces when the same query reads one.
    #[test]
    fn cte_exclusion_does_not_hide_a_genuine_both_table_reference() {
        // A CTE named `logs` alongside a genuine `logs` base-table read in a
        // different arm still surfaces `logs`: the base reference is real.
        let both = tables(
            "WITH t AS (SELECT value FROM samples) \
             SELECT * FROM t JOIN logs ON t.ts = logs.ts",
        );
        assert!(
            both.contains("samples") && both.contains("logs"),
            "a genuine base reference must survive CTE collection: {both:?}"
        );
    }

    fn reject_ddl(sql: &str) -> DdlValidationError {
        validate_ddl(sql).expect_err("must be rejected")
    }

    fn accept_create(sql: &str) -> (String, bool, bool, String, BTreeMap<String, String>) {
        match validate_ddl(sql).unwrap_or_else(|e| panic!("must be accepted: {sql}: {e}")) {
            DdlIntent::CreateExternal {
                name,
                if_not_exists,
                or_replace,
                location,
                options,
            } => (name, if_not_exists, or_replace, location, options),
            other => panic!("expected CreateExternal, got {other:?}"),
        }
    }

    #[test]
    fn plain_create_external_table_is_admitted() {
        let (name, if_not_exists, or_replace, location, options) = accept_create(
            "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/'",
        );
        assert_eq!(name, "orders");
        assert!(!if_not_exists);
        assert!(!or_replace);
        assert_eq!(location, "s3://bucket/prefix/");
        assert!(options.is_empty());
    }

    #[test]
    fn create_external_table_if_not_exists_is_admitted() {
        let (_, if_not_exists, or_replace, _, _) = accept_create(
            "CREATE EXTERNAL TABLE IF NOT EXISTS orders STORED AS PARQUET \
             LOCATION 's3://bucket/prefix/'",
        );
        assert!(if_not_exists);
        assert!(!or_replace);
    }

    #[test]
    fn create_or_replace_external_table_is_admitted() {
        let (_, if_not_exists, or_replace, _, _) = accept_create(
            "CREATE OR REPLACE EXTERNAL TABLE orders STORED AS PARQUET \
             LOCATION 's3://bucket/prefix/'",
        );
        assert!(!if_not_exists);
        assert!(or_replace);
    }

    #[test]
    fn create_external_table_single_object_location_is_admitted() {
        let (_, _, _, location, _) = accept_create(
            "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
             LOCATION 's3://bucket/prefix/single.parquet'",
        );
        assert_eq!(location, "s3://bucket/prefix/single.parquet");
    }

    #[test]
    fn create_external_table_options_are_admitted() {
        let (_, _, _, _, options) = accept_create(
            "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
             OPTIONS (binary_as_string 'true', 'ravel.cast.created_at' 'date-from-days')",
        );
        assert_eq!(
            options.get("binary_as_string").map(String::as_str),
            Some("true")
        );
        assert_eq!(
            options.get("ravel.cast.created_at").map(String::as_str),
            Some("date-from-days")
        );
    }

    #[test]
    fn drop_table_is_admitted() {
        match validate_ddl("DROP TABLE orders").expect("must be accepted") {
            DdlIntent::Drop { name, if_exists } => {
                assert_eq!(name, "orders");
                assert!(!if_exists);
            }
            other => panic!("expected Drop, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_if_exists_is_admitted() {
        match validate_ddl("DROP TABLE IF EXISTS orders").expect("must be accepted") {
            DdlIntent::Drop { name, if_exists } => {
                assert_eq!(name, "orders");
                assert!(if_exists);
            }
            other => panic!("expected Drop, got {other:?}"),
        }
    }

    #[test]
    fn plain_select_is_not_ddl() {
        assert!(matches!(
            reject_ddl("SELECT * FROM orders"),
            DdlValidationError::NotDdl {
                kind: "a SELECT statement"
            }
        ));
    }

    #[test]
    fn insert_is_not_ddl() {
        assert!(matches!(
            reject_ddl("INSERT INTO orders VALUES (1)"),
            DdlValidationError::NotDdl { .. }
        ));
    }

    #[test]
    fn copy_to_is_not_ddl() {
        assert!(matches!(
            reject_ddl("COPY (SELECT 1) TO 's3://evil/out.parquet'"),
            DdlValidationError::NotDdl { kind: "COPY" }
        ));
    }

    #[test]
    fn explain_is_not_ddl() {
        assert!(matches!(
            reject_ddl("EXPLAIN SELECT 1"),
            DdlValidationError::NotDdl { kind: "EXPLAIN" }
        ));
    }

    #[test]
    fn set_is_not_ddl() {
        assert!(matches!(
            reject_ddl("SET time_zone = 'UTC'"),
            DdlValidationError::NotDdl { .. }
        ));
    }

    #[test]
    fn multi_statement_ddl_body_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/'; \
                 DROP TABLE orders"
            ),
            DdlValidationError::MultipleStatements { count: 2 }
        ));
    }

    #[test]
    fn empty_ddl_body_is_rejected() {
        assert!(matches!(reject_ddl(""), DdlValidationError::Empty));
    }

    #[test]
    fn temporary_external_table_is_rejected() {
        // DataFusion's grammar places TEMPORARY after EXTERNAL, not before:
        // `CREATE EXTERNAL TEMPORARY TABLE ...`.
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TEMPORARY TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix/'"
            ),
            DdlValidationError::Temporary
        ));
    }

    #[test]
    fn unbounded_external_table_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE UNBOUNDED EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix/'"
            ),
            DdlValidationError::Unbounded
        ));
    }

    #[test]
    fn partitioned_by_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 PARTITIONED BY (day)"
            ),
            DdlValidationError::PartitionedBy
        ));
    }

    #[test]
    fn with_order_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 WITH ORDER (ts)"
            ),
            DdlValidationError::WithOrder
        ));
    }

    #[test]
    fn column_list_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders (a INT) STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix/'"
            ),
            DdlValidationError::ColumnList
        ));
    }

    #[test]
    fn non_parquet_stored_as_is_rejected() {
        assert!(matches!(
            reject_ddl("CREATE EXTERNAL TABLE orders STORED AS CSV LOCATION 's3://bucket/prefix/'"),
            DdlValidationError::NotParquet { file_type } if file_type == "CSV"
        ));
    }

    #[test]
    fn unsupported_option_key_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS (evil_option 'true')"
            ),
            DdlValidationError::UnsupportedOption { key } if key == "evil_option"
        ));
    }

    #[test]
    fn ravel_cast_option_with_empty_column_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS ('ravel.cast.' 'date-from-days')"
            ),
            DdlValidationError::UnsupportedOption { key } if key == "ravel.cast."
        ));
    }

    #[test]
    fn ravel_cast_option_with_a_quote_or_semicolon_in_the_column_is_rejected() {
        // The column name itself reaches `render_create_external` verbatim
        // (only the value is tokenized); a charset that admitted `'` or `;`
        // would let a crafted key carry injected SQL into redacted output.
        // `is_admitted_option_key` must refuse it before it ever gets there.
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS ('ravel.cast.x''); DROP TABLE evil--' 'date-from-days')"
            ),
            DdlValidationError::UnsupportedOption { key }
                if key == "ravel.cast.x'); DROP TABLE evil--"
        ));
    }

    #[test]
    fn ravel_cast_option_naming_a_mixed_case_column_is_admitted() {
        // ADR-2040 D5's own example (`ravel.cast.EventDate`): the cast
        // column rule is not the table-name rule, which admits lowercase
        // only and would wrongly refuse this.
        let (_, _, _, _, options) = accept_create(
            "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
             OPTIONS ('ravel.cast.EventDate' 'date-from-days')",
        );
        assert_eq!(
            options.get("ravel.cast.EventDate").map(String::as_str),
            Some("date-from-days")
        );
    }

    #[test]
    fn ravel_cast_option_with_punctuation_in_the_column_is_rejected() {
        for suffix in ["x'y", "x;y", "x.y", "x y"] {
            let key = format!("ravel.cast.{suffix}");
            let sql = format!(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS ('{}' 'date-from-days')",
                key.replace('\'', "''")
            );
            assert!(
                matches!(
                    reject_ddl(&sql),
                    DdlValidationError::UnsupportedOption { key: ref rejected } if *rejected == key
                ),
                "expected {key:?} to be rejected"
            );
        }
    }

    #[test]
    fn option_value_must_be_a_string_literal() {
        // DataFusion's OPTIONS grammar normalizes any bare word (quoted or
        // not, including keywords like `true`) to the same
        // `Value::SingleQuotedString`, so only a genuinely different token
        // kind -- here a bare number -- exercises this rejection.
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS (binary_as_string 123)"
            ),
            DdlValidationError::OptionValueNotString { key } if key == "binary_as_string"
        ));
    }

    #[test]
    fn binary_as_string_with_a_value_other_than_true_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS (binary_as_string 'yes')"
            ),
            DdlValidationError::InvalidOptionValue { key, value }
                if key == "binary_as_string" && value == "yes"
        ));
    }

    #[test]
    fn ravel_cast_with_an_unlisted_coercion_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS ('ravel.cast.created_at' 'to-the-moon')"
            ),
            DdlValidationError::InvalidOptionValue { key, value }
                if key == "ravel.cast.created_at" && value == "to-the-moon"
        ));
    }

    #[test]
    fn duplicate_options_key_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://bucket/prefix/' \
                 OPTIONS (binary_as_string 'true', binary_as_string 'true')"
            ),
            DdlValidationError::DuplicateOption { key } if key == "binary_as_string"
        ));
    }

    #[test]
    fn invalid_table_name_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE \"Orders\" STORED AS PARQUET LOCATION 's3://bucket/prefix/'"
            ),
            DdlValidationError::InvalidTableName(_)
        ));
    }

    #[test]
    fn reserved_table_name_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE samples STORED AS PARQUET LOCATION 's3://bucket/prefix/'"
            ),
            DdlValidationError::InvalidTableName(_)
        ));
    }

    #[test]
    fn drop_reserved_table_name_is_rejected() {
        assert!(matches!(
            reject_ddl("DROP TABLE logs"),
            DdlValidationError::InvalidTableName(_)
        ));
    }

    /// `name.to_string()` relies on `ObjectName`'s own `Display` joining a
    /// multi-part name with `.`, which `names::validate_table` then rejects
    /// as an invalid character -- so the refusal rests on a third-party
    /// `Display` detail rather than an explicit check of the name's part
    /// count. Pinned here so a future change to how the table name is
    /// extracted (e.g. taking only the last identifier) cannot silently
    /// start admitting a schema- or catalog-qualified name.
    #[test]
    fn create_external_table_with_schema_qualified_name_is_rejected() {
        for sql in [
            "CREATE EXTERNAL TABLE s.orders STORED AS PARQUET LOCATION 's3://bucket/prefix/'",
            "CREATE EXTERNAL TABLE c.s.orders STORED AS PARQUET LOCATION 's3://bucket/prefix/'",
        ] {
            assert!(
                matches!(reject_ddl(sql), DdlValidationError::InvalidTableName(_)),
                "{sql}"
            );
        }
    }

    /// Same rule as
    /// [`create_external_table_with_schema_qualified_name_is_rejected`], for
    /// `DROP TABLE`.
    #[test]
    fn drop_table_with_schema_qualified_name_is_rejected() {
        for sql in ["DROP TABLE s.orders", "DROP TABLE c.s.orders"] {
            assert!(
                matches!(reject_ddl(sql), DdlValidationError::InvalidTableName(_)),
                "{sql}"
            );
        }
    }

    #[test]
    fn location_without_scheme_is_rejected() {
        assert!(matches!(
            reject_ddl("CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION '/tmp/prefix/'"),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::NoScheme,
                ..
            })
        ));
    }

    #[test]
    fn location_as_a_relative_path_with_no_leading_slash_is_rejected() {
        // Same defect as the leading-slash case above, under a shape the
        // `split_once("://")` check cannot tell apart from a bucket name: a
        // bare relative path has no scheme either.
        assert!(matches!(
            reject_ddl("CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 'tmp/prefix/'"),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::NoScheme,
                ..
            })
        ));
    }

    #[test]
    fn location_with_file_scheme_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION 'file:///etc/passwd'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::UnsupportedScheme(ref scheme),
                ..
            }) if scheme == "file"
        ));
    }

    #[test]
    fn location_with_http_scheme_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 'http://bucket/prefix/'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::UnsupportedScheme(ref scheme),
                ..
            }) if scheme == "http"
        ));
    }

    #[test]
    fn location_with_dotdot_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/../prefix/'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::DotDot,
                ..
            })
        ));
    }

    #[test]
    fn location_with_empty_segment_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix//double/'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::EmptySegment,
                ..
            })
        ));
    }

    #[test]
    fn location_with_glob_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix/*.parquet'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::Glob,
                ..
            })
        ));
    }

    #[test]
    fn location_with_percent_escape_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix%2F/'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::PercentEscape,
                ..
            })
        ));
    }

    #[test]
    fn location_with_query_string_is_rejected() {
        assert!(matches!(
            reject_ddl(
                "CREATE EXTERNAL TABLE orders STORED AS PARQUET \
                 LOCATION 's3://bucket/prefix/?x=1'"
            ),
            DdlValidationError::Location(GrantsError::InvalidLocation {
                defect: LocationDefect::Query,
                ..
            })
        ));
    }

    #[test]
    fn drop_multiple_tables_is_rejected() {
        assert!(matches!(
            reject_ddl("DROP TABLE orders, other"),
            DdlValidationError::DropMultipleTables { count: 2 }
        ));
    }

    #[test]
    fn drop_view_is_rejected() {
        assert!(matches!(
            reject_ddl("DROP VIEW orders"),
            DdlValidationError::DropNotTable { .. }
        ));
    }

    #[test]
    fn drop_table_cascade_is_rejected() {
        assert!(matches!(
            reject_ddl("DROP TABLE orders CASCADE"),
            DdlValidationError::DropUnsupported { clause: "CASCADE" }
        ));
    }
}
