//! The frozen ClickBench query corpus (`benchmarks/clickbench/parquet/
//! queries.sql`), tamper-evidence over it, and the Ravel-side DDL template
//! and per-statement comparator overrides from `suite.toml`.

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
    /// 0-indexed projection columns the comparator ties on, in order, used
    /// when the statement's ORDER BY clause does not resolve to a
    /// projection index under the comparator's own textual rules.
    pub order_key: Vec<usize>,
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

    /// `suite.toml`'s one override (statement 43) loads with the exact key
    /// the comparator consumes.
    #[test]
    fn statement_43_override_loads() {
        let suite = load_default().expect("pinned corpus loads");
        let over = suite.override_for(43).expect("Q43 override present");
        assert_eq!(over.order_key, vec![0]);
        assert!(suite.override_for(1).is_none());
    }
}
