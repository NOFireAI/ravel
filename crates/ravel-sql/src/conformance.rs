//! SQL conformance registry and scored table generation (ADR-0035).
//!
//! ADR-0035 scores the query surface with a three-state classification per
//! construct, over the surface Ravel actually claims -- not all of DataFusion
//! SQL (Rejected Alternative A). This module is the SQL half's registry and
//! renderer: a static enumeration of every construct in Ravel's claimed
//! surface, each pinned to exactly one [`Classification`], plus the pure code
//! that renders the enumeration (annotated with a verified [`Verdict`] per
//! construct) into the markdown table published at `docs/sql-conformance.md`.
//!
//! The three states are ADR-0035's, applied to SQL:
//!
//! 1. [`Classification::SupportedAndCovered`]: implemented, with a passing test
//!    proving it. Here that is a construct the two-layer differential gate
//!    (`tests/pipeline.rs`, `tests/differential.rs`) already exercises against
//!    an independent reference, re-confirmed to execute by the conformance
//!    suite (`tests/conformance.rs`).
//! 2. [`Classification::IntentionallyRejected`]: refused with a typed error,
//!    never a panic and never silently-wrong data. This is the deliberate
//!    allowlist: the aggregates outside the admitted six
//!    ([`crate::validate::EXCLUDED_AGGREGATES`], ADR-0022 decision 2), the
//!    nondeterministic scalars ([`crate::session::EXCLUDED_SCALARS`]) and the
//!    excluded window functions ([`crate::session::EXCLUDED_WINDOWS`], ADR-0097
//!    decisions 4 and 6), every write/DDL statement (the read-only
//!    single-statement gate, [`crate::validate`]), and a query spanning both
//!    signal tables ([`crate::error::SqlError::CrossSignalQuery`], ADR-0033
//!    decision C).
//! 3. [`Classification::Unclassified`]: implemented but untested, or
//!    claimed-supported but actually wrong. This module declares no construct
//!    into this state; it is the state a construct *falls into* when its
//!    live verdict ([`Verdict::Broken`]) contradicts its declared
//!    classification, which is exactly the regression ADR-0035 wants a table
//!    diff to surface. The conformance suite fails if any row lands here.
//!
//! The score is `(state 1 + state 2) / total enumerated`, per ADR-0035: every
//! construct Ravel has taken a deliberate, verified position on, over the
//! enumerated surface. A future regression -- a rejected construct that starts
//! panicking, or a supported one that stops executing -- moves a row to state 3
//! and drops the score in the same change that caused it.
//!
//! The registry reads the excluded-aggregate set and the admitted set from
//! their single sources of truth ([`crate::validate::EXCLUDED_AGGREGATES`],
//! [`crate::session::ADMITTED_AGGREGATES`]) rather than restating them, so the
//! table cannot drift from what the gate actually enforces. The scalar and
//! window rows (ADR-0097 decision 8) are bounded rather than exhaustive: one
//! row per admitted scalar family plus Ravel's own UDFs, each of the eight
//! admitted window functions, and each excluded scalar/window function. Their
//! drift test (`registry_reads_the_live_allowlist_sets`) binds each registry's
//! rows to its constant per-registry: exact set equality for the window and
//! excluded-scalar registries, and family membership in
//! [`crate::session::ADMITTED_SCALARS`] for the admitted-scalar families.

use std::fmt::Write as _;

use crate::session::ADMITTED_AGGREGATES;
use crate::validate::EXCLUDED_AGGREGATES;

/// The three-state conformance classification (ADR-0035). Each enumerated
/// construct is declared into state 1 or state 2; state 3 is reserved for a
/// construct whose live behavior contradicts its declaration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    /// Implemented and proven by a passing test. `test` names the test that
    /// proves it.
    SupportedAndCovered {
        /// The test that proves the construct executes correctly.
        test: &'static str,
    },
    /// Deliberately refused with a typed error. `typed_error` names the error
    /// variant a caller receives (never a panic, never wrong data).
    IntentionallyRejected {
        /// The typed error variant the construct is rejected with.
        typed_error: &'static str,
    },
    /// Implemented but untested, or claimed-supported but actually wrong. No
    /// construct is declared here; it is where a construct lands when its live
    /// verdict contradicts its declaration.
    Unclassified,
}

impl Classification {
    /// The human-readable state label used in the published table.
    pub fn state_label(&self) -> &'static str {
        match self {
            Classification::SupportedAndCovered { .. } => "Supported and covered",
            Classification::IntentionallyRejected { .. } => "Intentionally rejected",
            Classification::Unclassified => "Unclassified / broken",
        }
    }
}

/// The family a construct belongs to, for grouping the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    /// An aggregate function (admitted or excluded).
    Aggregate,
    /// A query clause or operator (projection, filter, grouping, ordering,
    /// limit).
    Clause,
    /// A write, DDL, or other non-read-only statement.
    WriteStatement,
    /// The samples/logs table-dispatch behavior (ADR-0033).
    TableDispatch,
    /// A scalar function (admitted family representative, admitted Ravel UDF,
    /// or excluded nondeterministic scalar) (ADR-0097 decisions 4, 8).
    Scalar,
    /// A window function used with an `OVER` clause (admitted or excluded)
    /// (ADR-0097 decisions 6, 8).
    Window,
}

impl Category {
    /// A stable ordinal used to group and order the table deterministically.
    fn ordinal(self) -> u8 {
        match self {
            Category::Aggregate => 0,
            Category::Clause => 1,
            Category::Scalar => 2,
            Category::Window => 3,
            Category::WriteStatement => 4,
            Category::TableDispatch => 5,
        }
    }

    /// The human-readable category label used in the published table.
    pub fn label(self) -> &'static str {
        match self {
            Category::Aggregate => "Aggregate",
            Category::Clause => "Clause / operator",
            Category::Scalar => "Scalar function",
            Category::Window => "Window function",
            Category::WriteStatement => "Write / DDL statement",
            Category::TableDispatch => "Table dispatch",
        }
    }
}

/// One enumerated construct in Ravel's claimed SQL surface.
#[derive(Debug, Clone)]
pub struct Construct {
    /// The family the construct belongs to.
    pub category: Category,
    /// The construct's short name (the function name, statement kind, or
    /// dispatch case).
    pub name: String,
    /// A representative statement that exercises the construct.
    pub example: String,
    /// The construct's declared classification.
    pub classification: Classification,
    /// A one-line rationale, cross-linking the governing decision.
    pub rationale: &'static str,
}

/// The result of checking a construct's live behavior against its declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Live behavior matched the declared classification.
    Confirmed,
    /// Live behavior contradicted the declaration: a supported construct that
    /// did not execute, or a rejected construct that was not refused with the
    /// declared typed error. `observed` describes what actually happened.
    Broken {
        /// What the construct actually did, for the table's state-3 row.
        observed: String,
    },
}

/// A construct paired with its live verdict.
#[derive(Debug, Clone)]
pub struct Verified {
    /// The enumerated construct.
    pub construct: Construct,
    /// Its live verdict.
    pub verdict: Verdict,
}

impl Verified {
    /// The construct's effective classification: its declaration when the
    /// verdict confirms it, or [`Classification::Unclassified`] when the
    /// verdict contradicts it.
    pub fn effective(&self) -> Classification {
        match self.verdict {
            Verdict::Confirmed => self.construct.classification,
            Verdict::Broken { .. } => Classification::Unclassified,
        }
    }
}

// Test names used as evidence for the supported constructs. The conformance
// suite (`tests/conformance.rs`) re-confirms execution; the differential gate
// (`tests/differential.rs`) proves bit-exact correctness against an
// independent reference.
const T_SUPPORTED: &str = "tests/conformance.rs::supported_constructs_execute";
const T_DISPATCH: &str = "tests/conformance.rs::supported_constructs_execute";

// Typed-error variant names used as evidence for the rejected constructs.
const E_EXCLUDED_AGGREGATE: &str = "ValidationError::ExcludedAggregate";
const E_EXCLUDED_SCALAR: &str = "ValidationError::ExcludedScalar";
const E_EXCLUDED_WINDOW: &str = "ValidationError::ExcludedWindow";
const E_NOT_READ_ONLY: &str = "ValidationError::NotReadOnly";
const E_WRITE_IN_QUERY: &str = "ValidationError::WriteInQuery";
const E_CROSS_SIGNAL: &str = "SqlError::CrossSignalQuery";

/// One admitted upstream scalar *family* (ADR-0097 decision 8), attested by a
/// single representative row rather than one row per member. The family row is
/// deliberately NOT exhaustive: the other members of the family stay admitted
/// (they survive [`crate::session::build_session`]'s scalar gate and are held
/// exhaustive against the upstream registrations by the drift test in
/// `crate::session`), but they are covered by this one row and the drift test,
/// not individually attested here. Registering one row per admitted scalar
/// would mostly test upstream DataFusion rather than Ravel, which ADR-0097
/// decision 8 rejects.
struct ScalarFamily {
    /// The representative function, which must be a member of
    /// `ADMITTED_SCALARS` (asserted by the drift test). The datafusion-functions
    /// pack it stands in for is named in `note`.
    name: &'static str,
    /// A statement whose result is re-derived by hand from its literal inputs.
    example: &'static str,
    /// The re-derivation, quoted in the row's rationale.
    note: &'static str,
}

/// The six admitted scalar families (ADR-0097 decision 8: string, unicode,
/// datetime, math, regex, encoding), each attested by one representative whose
/// expected result is re-derived from its literal inputs in
/// `tests/conformance.rs`. Every representative is a member of
/// [`ADMITTED_SCALARS`]; the drift test asserts that.
const ADMITTED_SCALAR_FAMILIES: [ScalarFamily; 6] = [
    ScalarFamily {
        name: "upper",
        example: "SELECT upper('ab') FROM samples LIMIT 1",
        note: "string family representative: upper('ab') = 'AB'",
    },
    ScalarFamily {
        name: "reverse",
        example: "SELECT reverse('résumé') FROM samples LIMIT 1",
        note: "unicode family representative: reverse('résumé') = 'émusér' (character-wise, \
               not byte-wise)",
    },
    ScalarFamily {
        name: "to_char",
        example: "SELECT to_char(make_date(2024, 7, 15), '%Y-%m-%d') FROM samples LIMIT 1",
        note: "datetime family representative: to_char over a tz-free Date32 = '2024-07-15'",
    },
    ScalarFamily {
        name: "abs",
        example: "SELECT abs(-3.5) FROM samples LIMIT 1",
        note: "math family representative: abs(-3.5) = 3.5",
    },
    ScalarFamily {
        name: "regexp_replace",
        example: "SELECT regexp_replace('foo123bar', '[0-9]+', 'X') FROM samples LIMIT 1",
        note: "regex family representative: first-match replace of the digit run = 'fooXbar'",
    },
    ScalarFamily {
        name: "encode",
        example: "SELECT encode('a', 'hex') FROM samples LIMIT 1",
        note: "encoding family representative: hex of the byte 0x61 = '61'",
    },
];

/// Ravel's own scalar UDFs (ADR-0097 decision 8), each individually attested
/// because each is Ravel code rather than upstream DataFusion. Every name is a
/// member of [`ADMITTED_SCALARS`]; the drift test asserts that. Each carries a
/// real expectation over the conformance fixture in `tests/conformance.rs`:
/// `label`/`label_match` read the fixture's `__name__` label, `has_word` reads
/// the fixture's log bodies.
const RAVEL_SCALAR_UDFS: [(&str, &str); 3] = [
    (
        "label",
        "SELECT label(labels, '__name__') FROM samples WHERE value = 3.0",
    ),
    (
        "label_match",
        "SELECT count(*) FROM samples WHERE label_match(labels, '__name__', 'b')",
    ),
    (
        "has_word",
        "SELECT count(*) FROM logs WHERE has_word(body, '1')",
    ),
];

/// The eight admitted native window functions (ADR-0097 decision 6), each with
/// an `OVER` example whose aggregated result is re-derived from the four
/// fixture samples in `tests/conformance.rs`. Every name is a member of
/// [`ADMITTED_WINDOWS`]; the drift test asserts the set equality.
const ADMITTED_WINDOW_EXAMPLES: [(&str, &str); 8] = [
    (
        "row_number",
        "SELECT max(rn) FROM (SELECT row_number() OVER (ORDER BY value) AS rn FROM samples)",
    ),
    (
        "rank",
        "SELECT max(r) FROM (SELECT rank() OVER (ORDER BY value) AS r FROM samples)",
    ),
    (
        "dense_rank",
        "SELECT max(d) FROM (SELECT dense_rank() OVER (ORDER BY value) AS d FROM samples)",
    ),
    (
        "ntile",
        "SELECT max(n) FROM (SELECT ntile(2) OVER (ORDER BY value) AS n FROM samples)",
    ),
    (
        "lag",
        "SELECT count(p) FROM (SELECT lag(value) OVER (ORDER BY value) AS p FROM samples)",
    ),
    (
        "lead",
        "SELECT count(nx) FROM (SELECT lead(value) OVER (ORDER BY value) AS nx FROM samples)",
    ),
    (
        "cume_dist",
        "SELECT max(c) FROM (SELECT cume_dist() OVER (ORDER BY value) AS c FROM samples)",
    ),
    (
        "percent_rank",
        "SELECT max(pr) FROM (SELECT percent_rank() OVER (ORDER BY value) AS pr FROM samples)",
    ),
];

/// The `OVER` example for each excluded window function (ADR-0097 decision 6):
/// `first_value`/`last_value`/`nth_value`, refused with the window-aware typed
/// error. Their bare (aggregate-spelled) form is a separate, already-enumerated
/// `Aggregate` row that yields `ExcludedAggregate` instead (#373); both
/// spellings are attested because they produce different typed errors. The set
/// of names must equal [`EXCLUDED_WINDOWS`]; the drift test asserts that.
const EXCLUDED_WINDOW_EXAMPLES: [(&str, &str); 3] = [
    (
        "first_value",
        "SELECT first_value(value) OVER (ORDER BY ts) FROM samples",
    ),
    (
        "last_value",
        "SELECT last_value(value) OVER (ORDER BY ts) FROM samples",
    ),
    (
        "nth_value",
        "SELECT nth_value(value, 2) OVER (ORDER BY ts) FROM samples",
    ),
];

/// The excluded nondeterministic/environment-reading scalars (ADR-0097
/// decision 4), each with an example refused before planning by
/// `crate::validate` with the scalar-specific typed error. This is an
/// independent enumeration keyed by name to the constant it must match: the
/// drift test asserts this array's name set equals [`EXCLUDED_SCALARS`], so
/// adding an example without adding the name to the constant (or vice versa)
/// fails rather than passing tautologically.
///
/// The set has nine entries, not the eight ADR-0097 decision 4 names, because
/// `today` is a DataFusion alias of `current_date` that shares its wall-clock
/// read and its `EXCLUDED_SCALARS` membership; it is refused by the same typed
/// error and so earns a row. The three `current_*` niladic functions use their
/// bare SQL-standard spelling (which parses as a function call); the rest,
/// including `today`, take explicit parentheses.
const EXCLUDED_SCALAR_EXAMPLES: [(&str, &str); 9] = [
    ("uuid", "SELECT uuid() FROM samples"),
    ("random", "SELECT random() FROM samples"),
    ("rand", "SELECT rand() FROM samples"),
    ("now", "SELECT now() FROM samples"),
    ("current_timestamp", "SELECT current_timestamp FROM samples"),
    ("current_date", "SELECT current_date FROM samples"),
    ("current_time", "SELECT current_time FROM samples"),
    ("version", "SELECT version() FROM samples"),
    ("today", "SELECT today() FROM samples"),
];

/// A representative aggregate call over the metrics table.
fn agg_example(name: &str) -> String {
    format!("SELECT {name}(value) FROM samples")
}

/// The full enumeration of Ravel's claimed SQL surface, one construct per row,
/// each declared into exactly one [`Classification`].
///
/// The excluded-aggregate set and the admitted set are read from their single
/// sources of truth ([`crate::validate::EXCLUDED_AGGREGATES`],
/// [`crate::session::ADMITTED_AGGREGATES`]); this function never restates
/// either list.
pub fn registry() -> Vec<Construct> {
    let mut out = Vec::new();

    // --- Aggregates -------------------------------------------------------
    // The admitted six (ADR-0022 decision 2, avg/mean via the sequential-fold
    // UDAF, decisions 3/4): supported and covered by the differential gate.
    for name in ADMITTED_AGGREGATES {
        out.push(Construct {
            category: Category::Aggregate,
            name: name.to_string(),
            example: agg_example(name),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: "admitted aggregate (ADR-0022); exact against the differential gate",
        });
    }
    // Every other default DataFusion aggregate: rejected before planning with a
    // typed error naming the admitted set (ADR-0022 decision 2).
    for name in EXCLUDED_AGGREGATES {
        out.push(Construct {
            category: Category::Aggregate,
            name: name.to_string(),
            example: agg_example(name),
            classification: Classification::IntentionallyRejected {
                typed_error: E_EXCLUDED_AGGREGATE,
            },
            rationale: "outside the six-aggregate allowlist (ADR-0022 decision 2)",
        });
    }

    // --- Clauses / operators ---------------------------------------------
    // The operator subset the layer-2 differential gate covers.
    let clauses = [
        (
            "Projection",
            "SELECT ts, value FROM samples ORDER BY series_id, ts",
        ),
        (
            "Filter (WHERE)",
            "SELECT ts, value FROM samples WHERE value > 0 ORDER BY series_id, ts",
        ),
        (
            "GROUP BY",
            "SELECT series_id, count(value) FROM samples GROUP BY series_id ORDER BY series_id",
        ),
        ("ORDER BY", "SELECT ts, value FROM samples ORDER BY ts"),
        (
            "LIMIT",
            "SELECT ts, value FROM samples ORDER BY series_id, ts LIMIT 1",
        ),
    ];
    for (name, example) in clauses {
        out.push(Construct {
            category: Category::Clause,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: "covered by the two-layer differential gate (tests/differential.rs)",
        });
    }

    // The analytical clause/operator shapes typed-column queries need (ADR-0090
    // decision 8). None are blocked by `validate.rs` (it rejects statement
    // kinds, not clause shapes); each is registered SupportedAndCovered with a
    // hand-declared differential-oracle expectation (tests/conformance.rs),
    // re-derived directly from the fixture input, exactly like the base clause
    // rows above.
    let extended_clauses = [
        (
            "count(DISTINCT)",
            // `value` has no duplicates in the fixture, so a plain `count`
            // would be indistinguishable from a `count(DISTINCT)` over it;
            // `series_id` does (2 rows per series), so DISTINCT actually has
            // something to collapse.
            "SELECT count(DISTINCT series_id) FROM samples",
        ),
        (
            "OFFSET",
            "SELECT value FROM samples ORDER BY value OFFSET 1",
        ),
        (
            "HAVING",
            // Both series carry exactly 2 samples, so a threshold of `>= 2`
            // can't be told apart from no HAVING clause at all (both give 2
            // rows). `> 2` excludes every group, so a working filter yields 0
            // rows against the ungated 2.
            "SELECT series_id, count(value) FROM samples \
             GROUP BY series_id HAVING count(value) > 2",
        ),
        (
            "GROUP BY ordinal",
            "SELECT series_id, count(value) FROM samples GROUP BY 1 ORDER BY series_id",
        ),
        (
            "CASE",
            "SELECT CASE WHEN value > 0 THEN 1 ELSE 0 END FROM samples",
        ),
        ("IN list", "SELECT value FROM samples WHERE value IN (1, 2)"),
        (
            "REGEXP_REPLACE backreference",
            "SELECT regexp_replace('ab', '(a)(b)', '\\2\\1') FROM samples LIMIT 1",
        ),
        // The minute component of a timestamp. The `EXTRACT(minute FROM ts)`
        // sugar is not planner-wired in this crate's DataFusion `sql` feature
        // (no `ExprPlanner` for it, the same seam that leaves `attrs['k']` SQL
        // text unplannable); the equivalent `date_part` scalar function is, so
        // the minute-extraction capability is covered through that spelling.
        (
            "date_part(minute)",
            "SELECT date_part('minute', ts) FROM samples",
        ),
        ("DATE_TRUNC", "SELECT date_trunc('hour', ts) FROM samples"),
    ];
    for (name, example) in extended_clauses {
        out.push(Construct {
            category: Category::Clause,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: "analytical clause/operator over typed columns (ADR-0090 decision 8)",
        });
    }

    // Typed comparison and aggregate over a declared `i64` attribute column on
    // the `logs` table (ADR-0090 decisions 1 and 8): a real typed `>=` and a
    // real `SUM` over an `Int64` column, neither expressible over the
    // stringified `attrs` map. Verified against the conformance fixture's
    // declared `dur` column (tests/conformance.rs), whose expected values are
    // re-derived from the fixture's known `dur` inputs, not from this task's
    // own `merged_attrs`/`find_attr`.
    let typed_columns = [
        (
            "declared i64 typed comparison",
            "SELECT ts FROM logs WHERE dur >= 20",
        ),
        ("declared i64 typed aggregate", "SELECT sum(dur) FROM logs"),
    ];
    for (name, example) in typed_columns {
        out.push(Construct {
            category: Category::Clause,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: "typed predicate/aggregate over a declared column (ADR-0090)",
        });
    }

    // --- Scalar functions (ADR-0097 decisions 4, 8) ----------------------
    // One representative row per admitted upstream family. The other members
    // of each family stay admitted (they survive build_session's scalar gate,
    // held exhaustive against the upstream registrations by the drift test in
    // crate::session) but are covered by the family row and that drift test,
    // NOT individually attested here: 127 rows would mostly test upstream
    // DataFusion rather than Ravel (ADR-0097 decision 8).
    for fam in &ADMITTED_SCALAR_FAMILIES {
        out.push(Construct {
            category: Category::Scalar,
            name: fam.name.to_string(),
            example: fam.example.to_string(),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: fam.note,
        });
    }
    // Ravel's own scalar UDFs, individually attested: each is Ravel code, not
    // upstream DataFusion.
    for (name, example) in RAVEL_SCALAR_UDFS {
        out.push(Construct {
            category: Category::Scalar,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: "Ravel per-table scalar UDF, individually attested (ADR-0097 decision 8)",
        });
    }
    // The excluded nondeterministic/environment-reading scalars: refused before
    // planning with the scalar-specific typed error (ADR-0097 decisions 4, 9).
    for (name, example) in EXCLUDED_SCALAR_EXAMPLES {
        out.push(Construct {
            category: Category::Scalar,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::IntentionallyRejected {
                typed_error: E_EXCLUDED_SCALAR,
            },
            rationale: "nondeterministic or environment-reading; unattestable by the \
                        differential oracle (ADR-0097 decision 4)",
        });
    }

    // --- Window functions (ADR-0097 decisions 6, 8) ----------------------
    // The eight admitted native window functions, each verified through the
    // real pipeline against a result re-derived from the fixture samples.
    for (name, example) in ADMITTED_WINDOW_EXAMPLES {
        out.push(Construct {
            category: Category::Window,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::SupportedAndCovered { test: T_SUPPORTED },
            rationale: "admitted native window function (ADR-0097 decision 6)",
        });
    }
    // The three excluded window functions, refused with the window-aware typed
    // error. Their bare aggregate spelling is a separate Aggregate row above
    // that yields ExcludedAggregate instead (#373): both spellings are attested
    // because they produce different typed errors.
    for (name, example) in EXCLUDED_WINDOW_EXAMPLES {
        out.push(Construct {
            category: Category::Window,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::IntentionallyRejected {
                typed_error: E_EXCLUDED_WINDOW,
            },
            rationale: "excluded window function; bare spelling is a separate ExcludedAggregate \
                        row (ADR-0097 decision 6)",
        });
    }

    // --- Write / DDL statements ------------------------------------------
    // The read-only single-statement gate refuses every non-`SELECT` statement
    // DataFusion's front end parses, with a typed error before any planning
    // (crate::validate, security invariant 1). Each row is a statement kind the
    // parser accepts and the gate rejects.
    let write_stmts = [
        (
            "INSERT",
            "INSERT INTO samples VALUES (1, 2.0)",
            E_NOT_READ_ONLY,
        ),
        ("UPDATE", "UPDATE samples SET value = 1", E_NOT_READ_ONLY),
        ("DELETE", "DELETE FROM samples", E_NOT_READ_ONLY),
        ("CREATE TABLE", "CREATE TABLE t (a INT)", E_NOT_READ_ONLY),
        ("CREATE VIEW", "CREATE VIEW v AS SELECT 1", E_NOT_READ_ONLY),
        ("CREATE SCHEMA", "CREATE SCHEMA s", E_NOT_READ_ONLY),
        ("DROP", "DROP TABLE samples", E_NOT_READ_ONLY),
        (
            "ALTER TABLE",
            "ALTER TABLE samples ADD COLUMN x INT",
            E_NOT_READ_ONLY,
        ),
        (
            "CREATE EXTERNAL TABLE",
            "CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION '/tmp/x'",
            E_NOT_READ_ONLY,
        ),
        (
            "COPY",
            "COPY (SELECT 1) TO 's3://evil/out.parquet'",
            E_NOT_READ_ONLY,
        ),
        ("EXPLAIN", "EXPLAIN SELECT 1", E_NOT_READ_ONLY),
        (
            "SET",
            "SET datafusion.execution.batch_size = 1",
            E_NOT_READ_ONLY,
        ),
        ("Transaction control", "BEGIN TRANSACTION", E_NOT_READ_ONLY),
        ("PREPARE", "PREPARE p AS SELECT 1", E_NOT_READ_ONLY),
        (
            "Write nested in a query body",
            "WITH c AS (SELECT 1) INSERT INTO samples VALUES (1, 2.0)",
            E_WRITE_IN_QUERY,
        ),
    ];
    for (name, example, typed_error) in write_stmts {
        out.push(Construct {
            category: Category::WriteStatement,
            name: name.to_string(),
            example: example.to_string(),
            classification: Classification::IntentionallyRejected { typed_error },
            rationale: "read-only endpoint; refused before planning (crate::validate)",
        });
    }

    // --- Table dispatch (ADR-0033) ---------------------------------------
    out.push(Construct {
        category: Category::TableDispatch,
        name: "samples -> Signal::Metrics".to_string(),
        example: "SELECT ts, value FROM samples".to_string(),
        classification: Classification::SupportedAndCovered { test: T_DISPATCH },
        rationale: "metrics dispatch via referenced_base_tables (ADR-0033)",
    });
    out.push(Construct {
        category: Category::TableDispatch,
        name: "logs -> Signal::Logs".to_string(),
        example: "SELECT ts, body FROM logs".to_string(),
        classification: Classification::SupportedAndCovered { test: T_DISPATCH },
        rationale: "logs dispatch via referenced_base_tables (ADR-0033)",
    });
    out.push(Construct {
        category: Category::TableDispatch,
        name: "both tables (cross-signal)".to_string(),
        example: "SELECT * FROM samples JOIN logs ON samples.ts = logs.ts".to_string(),
        classification: Classification::IntentionallyRejected {
            typed_error: E_CROSS_SIGNAL,
        },
        rationale: "one signal per query in v1 (ADR-0033 decision C)",
    });

    out
}

/// The score over a verified surface: `(state 1 + state 2) / total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Score {
    /// Constructs classified [`Classification::SupportedAndCovered`].
    pub supported: usize,
    /// Constructs classified [`Classification::IntentionallyRejected`].
    pub rejected: usize,
    /// Constructs that landed in [`Classification::Unclassified`].
    pub unclassified: usize,
}

impl Score {
    /// The total enumerated construct count.
    pub fn total(&self) -> usize {
        self.supported + self.rejected + self.unclassified
    }

    /// The conformant count: states 1 and 2 combined.
    pub fn conformant(&self) -> usize {
        self.supported + self.rejected
    }

    /// The score as a percentage in tenths, e.g. `"100.0%"`. Integer math, so
    /// the rendered value is deterministic across platforms.
    pub fn percent(&self) -> String {
        let total = self.total();
        if total == 0 {
            return "0.0%".to_string();
        }
        let tenths = self.conformant() * 1000 / total;
        format!("{}.{}%", tenths / 10, tenths % 10)
    }
}

/// Tally the effective classifications over a verified surface.
pub fn score(verified: &[Verified]) -> Score {
    let mut s = Score {
        supported: 0,
        rejected: 0,
        unclassified: 0,
    };
    for v in verified {
        match v.effective() {
            Classification::SupportedAndCovered { .. } => s.supported += 1,
            Classification::IntentionallyRejected { .. } => s.rejected += 1,
            Classification::Unclassified => s.unclassified += 1,
        }
    }
    s
}

/// The evidence cell for a verified construct: the proving test for a supported
/// row, the typed error for a rejected row, or the observed behavior for a
/// broken row.
fn evidence(v: &Verified) -> String {
    match (&v.verdict, v.construct.classification) {
        (Verdict::Broken { observed }, _) => observed.clone(),
        (_, Classification::SupportedAndCovered { test }) => format!("`{test}`"),
        (_, Classification::IntentionallyRejected { typed_error }) => format!("`{typed_error}`"),
        (_, Classification::Unclassified) => "-".to_string(),
    }
}

/// Escape the `|` characters in an example so it renders inside a markdown
/// table cell without breaking the column layout.
fn cell(text: &str) -> String {
    text.replace('|', "\\|")
}

/// Render the complete `docs/sql-conformance.md` document from the verified
/// surface. The output is fully generated -- prose, table, and score -- so the
/// published file cannot silently drift from what the suite verifies
/// (ADR-0035: "the score is computed, not hand-maintained").
pub fn render_document(verified: &[Verified]) -> String {
    let mut rows = verified.to_vec();
    rows.sort_by(|a, b| {
        a.construct
            .category
            .ordinal()
            .cmp(&b.construct.category.ordinal())
            .then_with(|| a.construct.name.cmp(&b.construct.name))
    });
    let s = score(&rows);

    let mut out = String::new();
    let _ = write!(
        out,
        r#"# SQL conformance

<!--
  GENERATED FILE -- do not edit by hand.

  This document is rendered from the conformance registry in
  crates/ravel-sql/src/conformance.rs, annotated with the live verdict the
  conformance suite records for each construct. Regenerate with:

      REGEN_SQL_CONFORMANCE=1 cargo test -p ravel-sql --test conformance

  Editing the table by hand will be overwritten on the next run, and the
  suite fails if the committed file and the freshly rendered one disagree.
-->

This is the SQL half of the conformance scoring decided in
[ADR-0035](adrs/0035-conformance-scoring.md). It publishes what Ravel's SQL
surface supports, what it deliberately rejects, and what (if anything) is
neither -- scored over the surface Ravel actually claims, not over all of
DataFusion SQL (ADR-0035 Rejected Alternative A).

Ravel's SQL surface is a deliberately narrow allowlist, not general-purpose
SQL: a six-aggregate set, read-only `SELECT` only, and a single signal table
per query ([ADR-0033](adrs/0033-sql-query-over-logs.md),
[ADR-0022](adrs/0022-floating-aggregate-exactness.md)). Scoring against all of
DataFusion SQL would be meaningless (a tiny fixed percentage that never moves);
scoring against only the declared surface would read ~100% by construction. The
three-state scheme below is the resolution: enumerate the claimed surface, score
over the part with a deliberate verified position, and keep any unresolved
construct visible as an actionable miss.

## The three states

1. **Supported and covered** -- implemented, with a passing test proving it.
   These constructs are exercised by the two-layer differential gate
   (`crates/ravel-sql/tests/pipeline.rs` and
   `crates/ravel-sql/tests/differential.rs`) against an independent reference,
   and re-confirmed to execute by the conformance suite
   (`crates/ravel-sql/tests/conformance.rs`).
2. **Intentionally rejected** -- refused with a typed error, never a panic and
   never silently-wrong data. This is the allowlist working as designed: the
   aggregates outside the admitted six, every write/DDL statement, and a query
   spanning both signal tables. The conformance suite asserts each returns its
   declared typed error.
3. **Unclassified / broken** -- implemented but untested, or claimed-supported
   but actually wrong. No construct is declared into this state; a row lands
   here only when its live behavior contradicts its declaration, which fails
   the suite. A regression therefore shows up as a diff in this table in the
   same change that caused it.

The score is states 1 and 2 combined, over the total enumerated surface. A row
in state 3 lowers it; an out-of-scope construct Ravel rejects cleanly is state 2
and stays conformant.

## Score

- Supported and covered: {supported}
- Intentionally rejected: {rejected}
- Unclassified / broken: {unclassified}
- **Conformance: {conformant} / {total} = {percent}**

## Conformance table

| Category | Construct | Example | State | Evidence | Rationale |
| --- | --- | --- | --- | --- | --- |
"#,
        supported = s.supported,
        rejected = s.rejected,
        unclassified = s.unclassified,
        conformant = s.conformant(),
        total = s.total(),
        percent = s.percent(),
    );

    for v in &rows {
        let _ = writeln!(
            out,
            "| {category} | `{name}` | `{example}` | {state} | {evidence} | {rationale} |",
            category = v.construct.category.label(),
            name = cell(&v.construct.name),
            example = cell(&v.construct.example),
            state = v.effective().state_label(),
            evidence = evidence(v),
            rationale = v.construct.rationale,
        );
    }

    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn registry_declares_one_state_per_distinct_construct() {
        let registry = registry();
        assert!(!registry.is_empty());

        // A construct key is (category, name); each must appear exactly once,
        // so "exactly one state per enumerated construct" is well defined.
        let mut keys = std::collections::BTreeSet::new();
        for c in &registry {
            assert!(
                keys.insert((c.category.ordinal(), c.name.clone())),
                "duplicate construct: {:?} {}",
                c.category,
                c.name
            );
            // No construct is declared Unclassified: every row has a
            // deliberate state 1 or state 2 position.
            assert_ne!(
                c.classification,
                Classification::Unclassified,
                "{} must not be declared Unclassified",
                c.name
            );
        }
    }

    /// Every registry's row set corresponds to that registry's live constant,
    /// per-registry so a new registry's rows cannot silently escape the bind
    /// (ADR-0097 decision 8). The rule differs by registry, matching how each
    /// is enumerated:
    ///
    /// - Aggregate, admitted/excluded window, and excluded scalar: exact. The
    ///   set of row names must equal the constant. For windows and excluded
    ///   scalars the rows are built from independent example arrays keyed to
    ///   the constant by name, so this is a genuine two-way bind, not a
    ///   tautology: an example added without its constant name (or a constant
    ///   name without its example) fails here.
    /// - Admitted scalar: family-based. The bound is not one row per admitted
    ///   scalar (ADR-0097 decision 8 rejects that); it is one row per declared
    ///   family plus each Ravel UDF, and every such row must name a live member
    ///   of [`ADMITTED_SCALARS`]. A row naming a non-admitted scalar, or a
    ///   dropped family/UDF, fails here.
    #[test]
    fn registry_reads_the_live_allowlist_sets() {
        use std::collections::BTreeSet;

        use crate::session::{
            ADMITTED_SCALARS, ADMITTED_WINDOWS, EXCLUDED_SCALARS, EXCLUDED_WINDOWS,
        };

        let registry = registry();

        let names = |category: Category, rejected: bool| -> BTreeSet<String> {
            registry
                .iter()
                .filter(|c| {
                    c.category == category
                        && matches!(
                            c.classification,
                            Classification::IntentionallyRejected { .. }
                        ) == rejected
                })
                .map(|c| c.name.clone())
                .collect()
        };
        let constant =
            |xs: &[&str]| -> BTreeSet<String> { xs.iter().map(|s| s.to_string()).collect() };

        // Aggregate: exact count, as before (rows are built directly from the
        // constants, so a count bind catches a filtered-out row).
        assert_eq!(
            names(Category::Aggregate, true).len(),
            EXCLUDED_AGGREGATES.len(),
            "every excluded aggregate must be enumerated"
        );
        assert_eq!(
            names(Category::Aggregate, false).len(),
            ADMITTED_AGGREGATES.len(),
            "every admitted aggregate must be enumerated"
        );

        // Window: exact set equality against both constants.
        assert_eq!(
            names(Category::Window, false),
            constant(&ADMITTED_WINDOWS),
            "supported window rows must be exactly ADMITTED_WINDOWS"
        );
        assert_eq!(
            names(Category::Window, true),
            constant(&EXCLUDED_WINDOWS),
            "rejected window rows must be exactly EXCLUDED_WINDOWS"
        );

        // Excluded scalar: exact set equality against the constant.
        assert_eq!(
            names(Category::Scalar, true),
            constant(&EXCLUDED_SCALARS),
            "rejected scalar rows must be exactly EXCLUDED_SCALARS"
        );

        // Admitted scalar: family-based. One row per family plus each Ravel
        // UDF, each naming a live admitted scalar.
        let admitted_scalar_rows = names(Category::Scalar, false);
        assert_eq!(
            admitted_scalar_rows.len(),
            ADMITTED_SCALAR_FAMILIES.len() + RAVEL_SCALAR_UDFS.len(),
            "admitted scalar rows must be one per family plus each Ravel UDF"
        );
        for name in &admitted_scalar_rows {
            assert!(
                ADMITTED_SCALARS.contains(&name.as_str()),
                "admitted scalar row `{name}` is not a live member of ADMITTED_SCALARS"
            );
        }
        for fam in &ADMITTED_SCALAR_FAMILIES {
            assert!(
                admitted_scalar_rows.contains(fam.name),
                "scalar family representative `{}` has no row",
                fam.name
            );
        }
        for (udf, _) in RAVEL_SCALAR_UDFS {
            assert!(
                admitted_scalar_rows.contains(udf),
                "Ravel UDF `{udf}` has no row"
            );
        }
    }

    #[test]
    fn score_and_percent_are_deterministic_integer_math() {
        let s = Score {
            supported: 11,
            rejected: 34,
            unclassified: 0,
        };
        assert_eq!(s.total(), 45);
        assert_eq!(s.conformant(), 45);
        assert_eq!(s.percent(), "100.0%");

        let partial = Score {
            supported: 1,
            rejected: 2,
            unclassified: 1,
        };
        assert_eq!(partial.percent(), "75.0%");
    }

    #[test]
    fn a_broken_verdict_makes_a_row_effectively_unclassified() {
        let construct = Construct {
            category: Category::Aggregate,
            name: "median".to_string(),
            example: agg_example("median"),
            classification: Classification::IntentionallyRejected {
                typed_error: E_EXCLUDED_AGGREGATE,
            },
            rationale: "test",
        };
        let confirmed = Verified {
            construct: construct.clone(),
            verdict: Verdict::Confirmed,
        };
        assert!(matches!(
            confirmed.effective(),
            Classification::IntentionallyRejected { .. }
        ));
        let broken = Verified {
            construct,
            verdict: Verdict::Broken {
                observed: "returned a row".to_string(),
            },
        };
        assert_eq!(broken.effective(), Classification::Unclassified);
    }
}
