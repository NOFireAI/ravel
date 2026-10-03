//! The frozen ClickBench query corpus (`benchmarks/clickbench/parquet/
//! queries.sql`), tamper-evidence over it, and the Ravel-side DDL template
//! and per-statement comparator overrides from `suite.toml`.

use crate::clickbench_parquet::comparator::{ColumnMatch, FloatTolerance};
use serde::Deserialize;

/// The exact upstream `queries.sql` text, embedded at compile time so the
/// suite never reads it from a path that could differ between a dev
/// checkout and a packaged build.
const QUERIES_SQL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/clickbench/parquet/queries.sql"
));

/// `suite.toml`: the Ravel DDL template and per-statement comparator
/// overrides that accompany the frozen corpus above.
const SUITE_TOML: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/clickbench/parquet/suite.toml"
));

/// blake3 digest of the upstream `queries.sql` bytes, pinned so an edit to
/// that frozen file is refused rather than silently answered against
/// different queries. Upstream ships `queries.sql` with the SHA-256 digest
/// `7dfe5506deaf23e885492745a1533c1337f00d6cf07e3e5793bef20195299673`; this
/// is the blake3 digest of the identical bytes, since blake3 (not SHA-256)
/// is the hashing crate already pinned as a workspace dependency.
const QUERIES_BLAKE3: &str = "709cced0f8d6a780a5e754dea059b516f24efdeaad23223ca63a4c895f3a78fc";

/// Number of statements the frozen corpus holds. Pinned alongside the
/// digest so a test can assert the count independently of the digest check,
/// rather than one standing in for the other.
pub const STATEMENT_COUNT: usize = 43;

/// One ClickBench statement: its 1-based position in `queries.sql` and its
/// exact text, with the trailing `;` and surrounding whitespace stripped but
/// everything else preserved verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// 1-based position of this statement in `queries.sql`.
    pub number: u32,
    /// The statement text, without its trailing `;`.
    pub sql: String,
}

/// Everything loading the suite can fail on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SuiteError {
    /// The corpus text no longer matches the pinned blake3 digest.
    #[error(
        "benchmarks/clickbench/parquet/queries.sql: upstream text edited \
         (blake3 {found} does not match the pinned digest {expected})"
    )]
    DigestMismatch {
        /// blake3 digest of the text actually loaded.
        found: String,
        /// The pinned digest ([`QUERIES_BLAKE3`]).
        expected: &'static str,
    },
    /// `suite.toml` failed to parse.
    #[error("benchmarks/clickbench/parquet/suite.toml: {0}")]
    InvalidToml(String),
    /// A `[[statement]]` override declared exactly one of `float_reason` /
    /// `float_max_ulps`: a float tolerance needs both, since a reason with
    /// no bound or a bound with no explanation is not a usable declaration.
    #[error(
        "benchmarks/clickbench/parquet/suite.toml: statement {number} declares float_reason \
         without float_max_ulps, or vice versa; a float tolerance needs both"
    )]
    IncompleteFloatDeclaration { number: u32 },
    /// A `[[statement]]` override declared `compare` as something other than
    /// `"cardinality"`, the only supported value.
    #[error(
        "benchmarks/clickbench/parquet/suite.toml: statement {number} declares compare = \
         {value:?}; the only supported value is \"cardinality\""
    )]
    UnknownCompareMode { number: u32, value: String },
    /// A `[[statement]]` override declared `compare = "cardinality"` with no
    /// `reason`: a row-count-only comparison without a stated reason is not
    /// distinguishable from an override nobody checked.
    #[error(
        "benchmarks/clickbench/parquet/suite.toml: statement {number} declares compare = \
         \"cardinality\" without a reason"
    )]
    CardinalityWithoutReason { number: u32 },
    /// A `[[statement]]` override declared `ci_expected_error` with no
    /// `reason`: an expected failure needs to say what Ravel is missing.
    #[error(
        "benchmarks/clickbench/parquet/suite.toml: statement {number} declares \
         ci_expected_error without a reason"
    )]
    ExpectedErrorWithoutReason { number: u32 },
    /// A `[[statement]]` override declared `column_match` as something other
    /// than `"by-name"`, the only supported value.
    #[error(
        "benchmarks/clickbench/parquet/suite.toml: statement {number} declares column_match = \
         {value:?}; the only supported value is \"by-name\""
    )]
    UnknownColumnMatch { number: u32, value: String },
    /// A `[[statement]]` override declared `column_match = "by-name"` with no
    /// `reason`: comparing columns by name instead of by position needs to
    /// say why the two engines' column orders legitimately differ.
    #[error(
        "benchmarks/clickbench/parquet/suite.toml: statement {number} declares column_match = \
         \"by-name\" without a reason"
    )]
    ColumnMatchWithoutReason { number: u32 },
}

/// `suite.toml`'s `[table]` section: the Ravel DDL template that mounts the
/// fixture's `hits.parquet`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TableTemplate {
    /// The literal `CREATE EXTERNAL TABLE` statement text, with exactly one
    /// `{location}` placeholder standing in for the fixture's object URL.
    pub template: String,
}

impl TableTemplate {
    /// Substitute `{location}` with `location`, verbatim. `location` is not
    /// escaped: callers pass a trusted object-store URL produced by the
    /// fixture writer, never untrusted input.
    pub fn render(&self, location: &str) -> String {
        self.template.replace("{location}", location)
    }
}

/// A `suite.toml` `[[statement]]` override.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StatementOverride {
    /// 1-based statement number this override applies to.
    pub number: u32,
    /// 0-indexed projection columns the comparator ties on, in order. When
    /// set, this is the key outright: the comparator's textual ORDER BY
    /// rules are not consulted (`comparator::resolve_tie_spec`). Meant for a
    /// key the textual rules cannot resolve and [`Self::order_key_columns`]
    /// cannot name either (e.g. an ORDER BY expression with no corresponding
    /// output column name, as Q43's `DATE_TRUNC('minute', M)` has none
    /// under its alias `M`).
    #[serde(default)]
    pub order_key: Option<Vec<usize>>,
    /// Output column names the comparator ties on, in order, resolved at
    /// compare time by name against both the subject's and the reference's
    /// actual result schemas (both must carry every name). Used when the
    /// statement's projection is not textually stable enough for
    /// `order_key`'s positional form (e.g. Q24's `SELECT *`, where the
    /// column position a name lands on depends on the table's column order,
    /// not on anything in the SQL text itself).
    #[serde(default)]
    pub order_key_columns: Option<Vec<String>>,
    /// When set to `"cardinality"`, this statement compares only row and
    /// column counts; `reason` is then required and explains why no row
    /// identity can be recovered from the output (e.g. an ORDER BY column
    /// that is not projected, so rows past a tie cannot be told apart).
    /// `"cardinality"` is the only supported value; anything else is a
    /// typed load error ([`SuiteError::UnknownCompareMode`]).
    #[serde(default)]
    pub compare: Option<String>,
    /// When set to `"by-name"`, the comparator reorders the subject's columns
    /// to the reference's column order by output column name before
    /// comparing ([`ColumnMatch::ByName`]); `reason` is then required and
    /// explains why the two column orders differ. Absent means positional
    /// comparison. `"by-name"` is the only supported value; anything else is
    /// a typed load error ([`SuiteError::UnknownColumnMatch`]).
    #[serde(default)]
    pub column_match: Option<String>,
    /// Required alongside `compare = "cardinality"`
    /// ([`SuiteError::CardinalityWithoutReason`] otherwise), alongside
    /// `ci_expected_error` ([`SuiteError::ExpectedErrorWithoutReason`]
    /// otherwise), and alongside `column_match`
    /// ([`SuiteError::ColumnMatchWithoutReason`] otherwise); unused
    /// otherwise.
    #[serde(default)]
    pub reason: Option<String>,
    /// A stable part of the error Ravel returns for this statement, matched
    /// as a substring because each engine wraps the same failure in its own
    /// prefix (an HTTP status line, `query failed:`). When set, the statement
    /// is not compared: the acceptance test asserts that each Ravel arm fails
    /// with an error containing this text, so the statement starting to
    /// answer (or failing differently) turns it red.
    #[serde(default)]
    pub ci_expected_error: Option<String>,
    /// Why this statement's float cells are allowed to differ by up to
    /// `float_max_ulps` (e.g. a sequential-fold `avg`, ADR-0022). Declaring
    /// one of `float_reason`/`float_max_ulps` without the other is a typed
    /// load error ([`SuiteError::IncompleteFloatDeclaration`]).
    #[serde(default)]
    pub float_reason: Option<String>,
    /// The declared float tolerance, in ULPs of the ordered bit
    /// representation (same sign, both finite required; see
    /// [`FloatTolerance`]).
    #[serde(default)]
    pub float_max_ulps: Option<u32>,
}

impl StatementOverride {
    /// This override's declared [`FloatTolerance`], when it declares one.
    /// `load` already rejects a one-sided declaration, so by the time a
    /// caller sees a loaded `Suite` this is "both fields set" or "neither".
    pub fn float_tolerance(&self) -> Option<FloatTolerance> {
        match (&self.float_reason, self.float_max_ulps) {
            (Some(reason), Some(max_ulps)) => Some(FloatTolerance {
                reason: reason.clone(),
                max_ulps,
            }),
            _ => None,
        }
    }

    /// Whether this override declares `compare = "cardinality"`. `load`
    /// already rejects that declaration without a `reason`, so by the time a
    /// caller sees a loaded `Suite`, `true` here means [`Self::reason`] is
    /// `Some`.
    pub fn is_cardinality_only(&self) -> bool {
        self.compare.as_deref() == Some("cardinality")
    }

    /// The [`ColumnMatch`] this override declares: [`ColumnMatch::ByName`]
    /// for `column_match = "by-name"`, [`ColumnMatch::Positional`] when
    /// `column_match` is absent. `load` already rejects any other value and
    /// a by-name declaration without a `reason`.
    pub fn column_match(&self) -> ColumnMatch {
        if self.column_match.as_deref() == Some("by-name") {
            ColumnMatch::ByName
        } else {
            ColumnMatch::Positional
        }
    }
}

/// The on-disk shape of `suite.toml`, deserialized directly by serde before
/// [`Suite`] is assembled from it.
#[derive(Debug, Clone, Deserialize)]
struct SuiteTomlDoc {
    table: TableTemplate,
    #[serde(default, rename = "statement")]
    statements: Vec<StatementOverride>,
}

/// The loaded suite: the frozen query corpus, the DDL template, and the
/// per-statement overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Suite {
    /// Every statement in `queries.sql`, in file order.
    pub statements: Vec<Statement>,
    /// The Ravel DDL template from `suite.toml`'s `[table]` section.
    pub table: TableTemplate,
    /// Every `[[statement]]` override from `suite.toml`.
    pub overrides: Vec<StatementOverride>,
}

impl Suite {
    /// The override for statement `number`, if `suite.toml` declares one.
    pub fn override_for(&self, number: u32) -> Option<&StatementOverride> {
        self.overrides.iter().find(|o| o.number == number)
    }
}

/// Split `queries.sql`-shaped text into statements: one per non-blank line,
/// each with its trailing `;` and surrounding whitespace stripped. The
/// corpus format is one statement per line (never a statement spanning
/// lines, never two statements sharing a line), so this is a plain line
/// split rather than a SQL-aware tokenizer.
fn parse_statements(text: &str) -> Vec<Statement> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .enumerate()
        .map(|(i, line)| Statement {
            number: i as u32 + 1,
            sql: line.strip_suffix(';').unwrap_or(line).to_string(),
        })
        .collect()
}

/// Load the suite from the embedded `queries.sql` and `suite.toml` text.
pub fn load_default() -> Result<Suite, SuiteError> {
    load(QUERIES_SQL, SUITE_TOML)
}

/// Load the suite from caller-supplied text. Exposed separately from
/// [`load_default`] so a test can exercise the digest refusal against text
/// that deliberately diverges from the pinned corpus, without touching the
/// checked-in file.
pub fn load(queries_sql: &str, suite_toml: &str) -> Result<Suite, SuiteError> {
    let found = blake3::hash(queries_sql.as_bytes()).to_hex().to_string();
    if found != QUERIES_BLAKE3 {
        return Err(SuiteError::DigestMismatch {
            found,
            expected: QUERIES_BLAKE3,
        });
    }
    let doc: SuiteTomlDoc =
        toml::from_str(suite_toml).map_err(|e| SuiteError::InvalidToml(e.to_string()))?;
    for over in &doc.statements {
        if over.float_reason.is_some() != over.float_max_ulps.is_some() {
            return Err(SuiteError::IncompleteFloatDeclaration {
                number: over.number,
            });
        }
        if let Some(mode) = &over.compare {
            if mode != "cardinality" {
                return Err(SuiteError::UnknownCompareMode {
                    number: over.number,
                    value: mode.clone(),
                });
            }
            if over.reason.is_none() {
                return Err(SuiteError::CardinalityWithoutReason {
                    number: over.number,
                });
            }
        }
        if over.ci_expected_error.is_some() && over.reason.is_none() {
            return Err(SuiteError::ExpectedErrorWithoutReason {
                number: over.number,
            });
        }
        if let Some(mode) = &over.column_match {
            if mode != "by-name" {
                return Err(SuiteError::UnknownColumnMatch {
                    number: over.number,
                    value: mode.clone(),
                });
            }
            if over.reason.is_none() {
                return Err(SuiteError::ColumnMatchWithoutReason {
                    number: over.number,
                });
            }
        }
    }
    Ok(Suite {
        statements: parse_statements(queries_sql),
        table: doc.table,
        overrides: doc.statements,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// The frozen corpus holds exactly [`STATEMENT_COUNT`] statements. Pinned
    /// as a fact independent of the digest check: a corpus edit that kept the
    /// line count the same but changed statement text would fail the digest
    /// check already, but a corpus edit that only changed the line count is
    /// caught here.
    #[test]
    fn statement_count_is_exact() {
        let suite = load_default().expect("pinned corpus loads");
        assert_eq!(suite.statements.len(), STATEMENT_COUNT);
        assert_eq!(
            suite.statements.last().expect("non-empty corpus").number,
            STATEMENT_COUNT as u32
        );
    }

    /// The pinned blake3 digest holds against the checked-in file.
    #[test]
    fn digest_pin_holds() {
        let found = blake3::hash(QUERIES_SQL.as_bytes()).to_hex().to_string();
        assert_eq!(found, QUERIES_BLAKE3);
    }

    /// An edited corpus is refused, and the refusal names the edit rather
    /// than failing some other way (a parse error, a wrong count).
    #[test]
    fn edited_corpus_is_refused() {
        let tampered = QUERIES_SQL.replacen("SELECT COUNT(*) FROM hits;", "SELECT 1;", 1);
        let err = load(&tampered, SUITE_TOML).expect_err("tampered text must be refused");
        assert!(
            err.to_string().contains("upstream text edited"),
            "error must name the edit: {err}"
        );
    }

    /// Exactly 32 of the 43 statements carry a `LIMIT` clause (verified by
    /// `grep -c LIMIT benchmarks/clickbench/parquet/queries.sql` against the
    /// checked-in file), stated here as a fact about the loaded corpus.
    #[test]
    fn thirty_two_statements_carry_limit() {
        let suite = load_default().expect("pinned corpus loads");
        let with_limit = suite
            .statements
            .iter()
            .filter(|s| s.sql.contains("LIMIT"))
            .count();
        assert_eq!(with_limit, 32);
    }

    /// The table template renders with the placeholder substituted and
    /// nothing else disturbed.
    #[test]
    fn table_template_renders() {
        let suite = load_default().expect("pinned corpus loads");
        let rendered = suite.table.render("s3://bucket/hits.parquet");
        assert!(rendered.contains("s3://bucket/hits.parquet"));
        assert!(!rendered.contains("{location}"));
        assert!(rendered.contains("CREATE EXTERNAL TABLE hits"));
    }

    /// Statement 43's override loads with the exact key the comparator
    /// consumes, and declares no float tolerance.
    #[test]
    fn statement_43_override_loads() {
        let suite = load_default().expect("pinned corpus loads");
        let over = suite.override_for(43).expect("Q43 override present");
        assert_eq!(over.order_key, Some(vec![0]));
        assert!(suite.override_for(1).is_none());
        assert_eq!(over.float_tolerance(), None);
        assert!(!over.is_cardinality_only());
        assert_eq!(over.ci_expected_error, None);
    }

    /// The checked-in statement 4 override declares the sequential-fold avg
    /// tolerance at exactly 2 ULPs.
    #[test]
    fn statement_4_float_tolerance_loads() {
        let suite = load_default().expect("pinned corpus loads");
        let over = suite.override_for(4).expect("Q4 override present");
        let tolerance = over.float_tolerance().expect("Q4 declares a tolerance");
        assert_eq!(tolerance.reason, "Ravel sequential-fold avg (ADR-0022)");
        assert_eq!(tolerance.max_ulps, 2);
    }

    /// The checked-in statement 19 override declares its expected error text
    /// together with a reason.
    #[test]
    fn statement_19_expected_error_loads() {
        let suite = load_default().expect("pinned corpus loads");
        let over = suite.override_for(19).expect("Q19 override present");
        assert_eq!(
            over.ci_expected_error.as_deref(),
            Some("This feature is not implemented: Extract not supported by ExprPlanner")
        );
        assert_eq!(
            over.reason.as_deref(),
            Some(
                "EXTRACT has no ExprPlanner: datafusion is built without its datetime \
                 expressions (issue #2458)"
            )
        );
    }

    /// The checked-in statement 24 override compares columns by name, with
    /// its stated reason, and keeps its by-name ORDER BY key. No other
    /// statement compares by name.
    #[test]
    fn statement_24_column_match_loads() {
        let suite = load_default().expect("pinned corpus loads");
        let over = suite.override_for(24).expect("Q24 override present");
        assert_eq!(over.column_match(), ColumnMatch::ByName);
        assert_eq!(
            over.reason.as_deref(),
            Some(
                "upstream's view moves EventDate last; Ravel's ravel.cast.EventDate casts it \
                 in place at its Parquet position"
            )
        );
        assert_eq!(over.order_key_columns, Some(vec!["EventTime".to_string()]));
        let by_name: Vec<u32> = suite
            .overrides
            .iter()
            .filter(|o| o.column_match() == ColumnMatch::ByName)
            .map(|o| o.number)
            .collect();
        assert_eq!(by_name, vec![24]);
    }

    /// `column_match = "by-name"` with no `reason` is a typed load error.
    #[test]
    fn column_match_without_reason_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 24
            column_match = "by-name"
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("reason-less column_match is refused");
        assert!(matches!(
            err,
            SuiteError::ColumnMatchWithoutReason { number: 24 }
        ));
    }

    /// A `column_match` value other than `"by-name"` is a typed load error.
    #[test]
    fn unknown_column_match_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 24
            column_match = "by_name"
            reason = "typo for by-name"
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("unknown column_match is refused");
        assert!(matches!(
            err,
            SuiteError::UnknownColumnMatch { number: 24, .. }
        ));
    }

    /// An override without `column_match` compares positionally.
    #[test]
    fn absent_column_match_is_positional() {
        let suite = load_default().expect("pinned corpus loads");
        let over = suite.override_for(43).expect("Q43 override present");
        assert_eq!(over.column_match, None);
        assert_eq!(over.column_match(), ColumnMatch::Positional);
    }

    /// `ci_expected_error` with no `reason` is a typed load error.
    #[test]
    fn expected_error_without_reason_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 19
            ci_expected_error = "query failed"
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("reason-less expected error is refused");
        assert!(matches!(
            err,
            SuiteError::ExpectedErrorWithoutReason { number: 19 }
        ));
    }

    /// A `[[statement]]` block declaring only `float_reason` (no
    /// `float_max_ulps`) is a typed load error, not a silently-ignored
    /// field or a panic.
    #[test]
    fn float_reason_without_max_ulps_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 1
            order_key = [0]
            float_reason = "test"
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("one-sided float declaration is refused");
        assert!(matches!(
            err,
            SuiteError::IncompleteFloatDeclaration { number: 1 }
        ));
    }

    /// The mirror of the above: `float_max_ulps` with no `float_reason` is
    /// refused the same way.
    #[test]
    fn float_max_ulps_without_reason_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 1
            order_key = [0]
            float_max_ulps = 1
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("one-sided float declaration is refused");
        assert!(matches!(
            err,
            SuiteError::IncompleteFloatDeclaration { number: 1 }
        ));
    }

    /// A complete `float_reason`/`float_max_ulps` pair loads and builds the
    /// comparator's `FloatTolerance` from it.
    #[test]
    fn complete_float_declaration_builds_tolerance() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 1
            order_key = [0]
            float_reason = "sequential-fold avg rounding"
            float_max_ulps = 4
        "#;
        let suite = load(QUERIES_SQL, toml).expect("complete declaration loads");
        let over = suite.override_for(1).expect("override present");
        let tolerance = over.float_tolerance().expect("both fields set");
        assert_eq!(tolerance.reason, "sequential-fold avg rounding");
        assert_eq!(tolerance.max_ulps, 4);
    }

    /// A `[[statement]]` block naming an `order_key_columns` list (rather
    /// than a positional `order_key`) loads with no `order_key` at all.
    #[test]
    fn order_key_columns_override_loads() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 24
            order_key_columns = ["EventTime"]
        "#;
        let suite = load(QUERIES_SQL, toml).expect("order_key_columns override loads");
        let over = suite.override_for(24).expect("override present");
        assert_eq!(over.order_key, None);
        assert_eq!(over.order_key_columns, Some(vec!["EventTime".to_string()]));
        assert!(!over.is_cardinality_only());
    }

    /// A `compare = "cardinality"` block with its required `reason` loads,
    /// and `is_cardinality_only` reports it.
    #[test]
    fn cardinality_compare_override_loads() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 25
            compare = "cardinality"
            reason = "ORDER BY key not projected"
        "#;
        let suite = load(QUERIES_SQL, toml).expect("cardinality override loads");
        let over = suite.override_for(25).expect("override present");
        assert!(over.is_cardinality_only());
        assert_eq!(over.reason.as_deref(), Some("ORDER BY key not projected"));
    }

    /// `compare = "cardinality"` with no `reason` is a typed load error, not
    /// a silently-unexplained override.
    #[test]
    fn cardinality_compare_without_reason_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 25
            compare = "cardinality"
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("reason-less cardinality is refused");
        assert!(matches!(
            err,
            SuiteError::CardinalityWithoutReason { number: 25 }
        ));
    }

    /// A `compare` value other than `"cardinality"` is a typed load error.
    #[test]
    fn unknown_compare_mode_is_refused() {
        let toml = r#"
            [table]
            template = "CREATE EXTERNAL TABLE hits () STORED AS PARQUET LOCATION '{location}'"

            [[statement]]
            number = 25
            compare = "rowcount"
            reason = "typo for cardinality"
        "#;
        let err = load(QUERIES_SQL, toml).expect_err("unknown compare mode is refused");
        assert!(matches!(
            err,
            SuiteError::UnknownCompareMode { number: 25, .. }
        ));
    }
}
