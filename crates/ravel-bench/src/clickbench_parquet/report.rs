//! The ClickBench Parquet lane's report (ADR-2040 D7, issue #2055): what one
//! run of `clickbench_parquet_bench` measured, the provenance that makes it
//! comparable, the pre-registration it is judged against
//! (`benchmarks/clickbench/parquet/prereg.toml`), and [`check`], which turns
//! D7's bar into typed [`Violation`]s.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use datafusion::arrow::record_batch::RecordBatch;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

use super::comparator::{
    self, ColumnMatch, ComparisonReport, FloatMismatch, Verdict, resolve_order_key_columns,
    resolve_tie_spec,
};
use super::concurrency::ConcurrencyFigures;
use super::suite::{self, STATEMENT_COUNT, Statement, StatementOverride, Suite};

/// Which layout `hits` was mounted from (ADR-2040 D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Arm {
    /// The `hits/` prefix of 100 partitioned files.
    A,
    /// The single `hits.parquet` file.
    B,
}

impl Arm {
    /// The number of files `CREATE EXTERNAL TABLE` must report mounting: the
    /// 100 `hits_*.parquet` parts D7 names for arm A, one file for arm B.
    pub fn expected_files(self) -> u64 {
        match self {
            Arm::A => 100,
            Arm::B => 1,
        }
    }

    /// Refuses a `LOCATION` that does not name this arm's layout: arm A is a
    /// prefix (ends in `/`), arm B is the single `hits.parquet` object.
    pub fn check_location(self, location: &str) -> Result<(), String> {
        let fits = match self {
            Arm::A => location.ends_with('/'),
            Arm::B => location.ends_with("/hits.parquet"),
        };
        if fits {
            Ok(())
        } else {
            Err(match self {
                Arm::A => {
                    format!("arm a mounts a prefix, so --location must end in '/': {location}")
                }
                Arm::B => format!(
                    "arm b mounts the single file, so --location must end in '/hits.parquet': \
                     {location}"
                ),
            })
        }
    }
}

/// The server settings a report is stamped with, each read from the
/// server's startup log. `cache_max_bytes` and `fetch_concurrency` are the
/// fetch-cache and fetch-path figures D7 stamps, `sql_max_query_bytes` is
/// the per-query memory cap, and the other two are the remaining budgets
/// the run depends on.
pub const STAMPED_SETTINGS: [&str; 5] = [
    "cache_max_bytes",
    "catalog_cache_max_bytes",
    "fetch_concurrency",
    "sql_max_query_bytes",
    "sql_tenant_max_bytes",
];

/// One stamped server setting as found in the server log: every value a
/// `performance default resolved` line for it carried, and every such line
/// whose `value=` could not be read. [`check`] requires exactly one value and
/// no unreadable line.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerSetting {
    pub setting: String,
    pub values: Vec<u64>,
    pub unreadable_lines: Vec<String>,
}

impl ServerSetting {
    /// The setting's value when the log carried it exactly once.
    pub fn single(&self) -> Option<u64> {
        match (self.values.as_slice(), self.unreadable_lines.is_empty()) {
            ([value], true) => Some(*value),
            _ => None,
        }
    }
}

/// Drops ANSI SGR escape sequences (`ESC [ ... m`). `tracing_subscriber`'s
/// default `fmt` layer colours field names even when stdout is redirected to
/// a file, so a captured server log carries them around every `key=`.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// The text after `key=` in a `tracing` fmt line, up to the next space. A
/// key matches only at the start of a field, so `setting=` is not found
/// inside `xsetting=`.
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!(" {key}=");
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    Some(rest.split(' ').next().unwrap_or(rest))
}

/// Reads [`STAMPED_SETTINGS`] out of a `ravel-server` startup log.
///
/// The lines are the ones `ResolvedPerformanceDefaults::emit` writes
/// (services/ravel-server/src/config.rs, `fn emit` at line 3765; the
/// `fetch_concurrency` line at 3772, `cache_max_bytes` at 3820,
/// `catalog_cache_max_bytes` at 3826, `sql_max_query_bytes` at 3906 and
/// `sql_tenant_max_bytes` at 3917), through the default `tracing_subscriber`
/// fmt layer:
///
/// ```text
/// 2026-10-03T10:00:00.000000Z  INFO ravel_server::config: performance default resolved setting="cache_max_bytes" value=8053063680 source="derived"
/// ```
///
/// A line counts when it carries the message `performance default resolved`
/// and a `setting="<name>"` field naming the setting exactly; its `value=`
/// field must be a plain unsigned integer.
pub fn parse_server_settings(log: &str) -> Vec<ServerSetting> {
    let mut settings: Vec<ServerSetting> = STAMPED_SETTINGS
        .iter()
        .map(|name| ServerSetting {
            setting: (*name).to_string(),
            values: Vec::new(),
            unreadable_lines: Vec::new(),
        })
        .collect();
    for raw in log.lines() {
        let line = strip_ansi(raw);
        if !line.contains(" performance default resolved ") {
            continue;
        }
        let Some(name) = field(&line, "setting") else {
            continue;
        };
        let name = name.trim_matches('"');
        let Some(entry) = settings.iter_mut().find(|s| s.setting == name) else {
            continue;
        };
        match field(&line, "value").and_then(|v| v.parse::<u64>().ok()) {
            Some(value) => entry.values.push(value),
            None => entry.unreadable_lines.push(line.trim().to_string()),
        }
    }
    settings
}

/// Where a run's figures came from. Without it, two reports cannot be
/// compared (ADR-2040 D7 stamps every figure with the cache size and the
/// memory cap).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    /// `bench_env::git_commit` at run time.
    pub git_sha: String,
    /// Path of the bench binary that ran.
    pub binary: String,
    pub arm: Arm,
    /// The `LOCATION` the table was mounted from.
    pub location: String,
    /// The server URL the run measured.
    pub server: String,
    /// The `--server-log` path the stamps were read from.
    pub server_log: String,
    /// One entry per [`STAMPED_SETTINGS`] name, in that order.
    pub server_settings: Vec<ServerSetting>,
    /// `--sql-max-query-bytes` as given to the bench.
    pub sql_max_query_bytes: u64,
    /// `--sql-tenant-max-bytes` as given to the bench.
    pub sql_tenant_max_bytes: u64,
    /// The `--reference` directory, and its `VERSION` file's text.
    pub reference_dir: String,
    pub reference_version: String,
    /// What `CREATE EXTERNAL TABLE` reported mounting.
    pub ddl_files: Option<u64>,
}

/// [`comparator::Verdict`] in serialisable form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum VerdictRecord {
    Pass,
    CardinalityOnly { reason: Option<String> },
    Fail,
}

impl From<&Verdict> for VerdictRecord {
    fn from(verdict: &Verdict) -> Self {
        match verdict {
            Verdict::Pass => VerdictRecord::Pass,
            Verdict::CardinalityOnly(reason) => VerdictRecord::CardinalityOnly {
                reason: reason.clone(),
            },
            Verdict::Fail => VerdictRecord::Fail,
        }
    }
}

impl fmt::Display for VerdictRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VerdictRecord::Pass => f.write_str("Pass"),
            VerdictRecord::CardinalityOnly { reason: None } => f.write_str("CardinalityOnly"),
            VerdictRecord::CardinalityOnly {
                reason: Some(reason),
            } => write!(f, "CardinalityOnly({reason})"),
            VerdictRecord::Fail => f.write_str("Fail"),
        }
    }
}

/// One float cell that differed from the reference, as the comparator
/// listed it. `explanation` is set only when it fell inside the statement's
/// declared tolerance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FloatMismatchRecord {
    pub column: usize,
    /// The matched row, `Debug`-formatted.
    pub row: String,
    pub reference_bits: u64,
    pub subject_bits: u64,
    pub reference_f64: f64,
    pub subject_f64: f64,
    pub explanation: Option<String>,
}

impl From<&FloatMismatch> for FloatMismatchRecord {
    fn from(m: &FloatMismatch) -> Self {
        FloatMismatchRecord {
            column: m.column,
            row: format!("{:?}", m.row_key),
            reference_bits: m.reference_bits,
            subject_bits: m.subject_bits,
            reference_f64: m.reference_f64,
            subject_f64: m.subject_f64,
            explanation: m.explanation.clone(),
        }
    }
}

/// One statement's figures.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatementFigures {
    /// 1-based position in `queries.sql`.
    pub number: u32,
    /// Try 1, in seconds; `None` when the statement failed.
    pub cold_s: Option<f64>,
    /// The better of tries 2 and 3, in seconds; `None` when it failed.
    pub hot_s: Option<f64>,
    /// The comparator's verdict on try 1 against the reference; `None` when
    /// the statement failed or the comparison could not run.
    pub verdict: Option<VerdictRecord>,
    pub float_mismatches: Vec<FloatMismatchRecord>,
    /// Rows the comparator listed as missing from or extra in Ravel's answer
    /// (it lists at most 20 of each).
    pub missing_rows: usize,
    pub extra_rows: usize,
    /// Why the comparison could not run, when it could not.
    pub compare_error: Option<String>,
    /// The first error any try returned. A statement with an error is a
    /// failed statement.
    pub error: Option<String>,
}

impl StatementFigures {
    /// A statement whose tries all answered: `tries` are its three wall
    /// times in seconds, and `comparison` is try 1 judged against the
    /// reference.
    pub fn answered(
        number: u32,
        tries: [f64; 3],
        comparison: Result<ComparisonReport, String>,
    ) -> Self {
        let [cold, second, third] = tries;
        let mut figures = StatementFigures {
            number,
            cold_s: Some(cold),
            hot_s: Some(second.min(third)),
            verdict: None,
            float_mismatches: Vec::new(),
            missing_rows: 0,
            extra_rows: 0,
            compare_error: None,
            error: None,
        };
        match comparison {
            Ok(report) => {
                figures.verdict = Some(VerdictRecord::from(&report.verdict));
                figures.float_mismatches = report
                    .float_mismatches
                    .iter()
                    .map(FloatMismatchRecord::from)
                    .collect();
                figures.missing_rows = report.row_mismatch.missing.len();
                figures.extra_rows = report.row_mismatch.extra.len();
            }
            Err(error) => figures.compare_error = Some(error),
        }
        figures
    }

    /// A statement at least one try of which failed with `error`.
    pub fn failed(number: u32, error: String) -> Self {
        StatementFigures {
            number,
            cold_s: None,
            hot_s: None,
            verdict: None,
            float_mismatches: Vec::new(),
            missing_rows: 0,
            extra_rows: 0,
            compare_error: None,
            error: Some(error),
        }
    }
}

/// Sums over the statements that answered, and the ones that did not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Totals {
    pub cold_sum_s: f64,
    pub hot_sum_s: f64,
    pub answered: usize,
    pub failed: Vec<u32>,
}

impl Totals {
    pub fn from_statements(statements: &[StatementFigures]) -> Self {
        let mut totals = Totals {
            cold_sum_s: 0.0,
            hot_sum_s: 0.0,
            answered: 0,
            failed: Vec::new(),
        };
        for statement in statements {
            if statement.error.is_some() {
                totals.failed.push(statement.number);
                continue;
            }
            totals.answered += 1;
            totals.cold_sum_s += statement.cold_s.unwrap_or(0.0);
            totals.hot_sum_s += statement.hot_s.unwrap_or(0.0);
        }
        totals
    }
}

/// One run's report, written to `--out` as JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClickBenchParquetReport {
    pub provenance: Provenance,
    pub statements: Vec<StatementFigures>,
    pub totals: Totals,
    /// Pre-registered failures that answered instead. Not a violation, but a
    /// finding to post with the report.
    pub registered_failures_answered: Vec<u32>,
    /// `None` when no concurrency phase ran (`--concurrency-seconds 0`) or
    /// when it failed after it started.
    pub concurrency: Option<ConcurrencyFigures>,
    /// Why the concurrency phase failed after it started (a task panicked
    /// or stopped abnormally); `None` when it completed or did not run.
    pub concurrency_error: Option<String>,
}

/// Statements in `prereg.failures` that the report shows answering.
pub fn registered_failures_answered(statements: &[StatementFigures], prereg: &Prereg) -> Vec<u32> {
    statements
        .iter()
        .filter(|s| s.error.is_none() && prereg.failures.contains(&s.number))
        .map(|s| s.number)
        .collect()
}

/// The text `prereg.toml` holds for a figure not measured yet.
pub const PLACEHOLDER: &str = "FILL-IN-ON-REFERENCE-MACHINE";

/// Keys the operator fills in on the reference machine before the first
/// subject run. Each ships as [`PLACEHOLDER`].
pub const OPERATOR_KEYS: [&str; 3] = ["memory_cap_bytes", "arm_b_hot_s", "arm_b_cold_s"];

const ALL_KEYS: [&str; 6] = [
    "memory_cap_bytes",
    "arm_b_hot_s",
    "arm_b_cold_s",
    "failures",
    "rlog_hot_ceiling_s",
    "concurrency_qps_floor",
];

/// Keys an earlier `prereg.toml` carried, each with why it went. A file that
/// still holds one is refused, so a stale copy is never judged as if the bar
/// it names were still checked.
const REMOVED_KEYS: [(&str, &str); 1] = [(
    "concurrency_error_ratio_ceiling",
    "D7's concurrency bar is the queries-per-second floor and no error from a \
     statement outside `failures`; the error ratio is reported, not judged",
)];

/// ADR-2023's concurrency error ratio on the RLOG entry. Printed beside the
/// phase's `error_ratio` for comparison only: the two are measured over
/// different statement sets, so neither is a bar for the other.
pub const RLOG_ERROR_RATIO: f64 = 0.101;

/// `prereg.toml`, loaded and validated.
#[derive(Debug, Clone, PartialEq)]
pub struct Prereg {
    /// The per-query memory cap the failures are registered against; the
    /// server's logged `sql_max_query_bytes` must equal it.
    pub memory_cap_bytes: u64,
    pub arm_b_hot_s: f64,
    pub arm_b_cold_s: f64,
    /// Statement numbers allowed to fail.
    pub failures: BTreeSet<u32>,
    pub rlog_hot_ceiling_s: f64,
    pub concurrency_qps_floor: f64,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PreregError {
    #[error("cannot read {path}: {error}")]
    Read { path: String, error: String },
    #[error("prereg is not valid TOML: {0}")]
    Parse(String),
    #[error(
        "prereg keys still hold the placeholder {PLACEHOLDER:?}: {}; fill them in on the \
         reference machine before the first subject run",
        keys.join(", ")
    )]
    Unfilled { keys: Vec<String> },
    #[error("prereg key {key} is missing")]
    Missing { key: String },
    #[error("prereg key {key} is not one this lane reads")]
    UnknownKey { key: String },
    #[error("prereg key {key} was removed (issue #2055): {reason}; delete it from the file")]
    RemovedKey { key: String, reason: String },
    #[error("prereg key {key}: {reason}")]
    Invalid { key: String, reason: String },
}

fn invalid(key: &str, reason: impl Into<String>) -> PreregError {
    PreregError::Invalid {
        key: key.to_string(),
        reason: reason.into(),
    }
}

fn positive_number(table: &toml::Table, key: &str) -> Result<f64, PreregError> {
    let value = match table.get(key) {
        Some(toml::Value::Float(f)) => *f,
        Some(toml::Value::Integer(i)) => *i as f64,
        Some(other) => return Err(invalid(key, format!("expected a number, found {other}"))),
        None => {
            return Err(PreregError::Missing {
                key: key.to_string(),
            });
        }
    };
    if !value.is_finite() || value <= 0.0 {
        return Err(invalid(
            key,
            format!("must be finite and above 0, found {value}"),
        ));
    }
    Ok(value)
}

/// Parses `q<N>` with N in 1..=43.
fn statement_label(label: &str) -> Option<u32> {
    let digits = label.strip_prefix('q')?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let number = digits.parse::<u32>().ok()?;
    (1..=STATEMENT_COUNT as u32)
        .contains(&number)
        .then_some(number)
}

/// Parses and validates `prereg.toml` text. Every [`OPERATOR_KEYS`] entry
/// still holding [`PLACEHOLDER`] is named in one [`PreregError::Unfilled`];
/// no key has a default, and a zero, negative or non-finite figure is
/// refused.
pub fn parse_prereg(text: &str) -> Result<Prereg, PreregError> {
    let table: toml::Table = toml::from_str(text).map_err(|e| PreregError::Parse(e.to_string()))?;
    if let Some((key, reason)) = REMOVED_KEYS
        .iter()
        .find(|(key, _)| table.contains_key(*key))
    {
        return Err(PreregError::RemovedKey {
            key: (*key).to_string(),
            reason: (*reason).to_string(),
        });
    }
    if let Some(key) = table.keys().find(|k| !ALL_KEYS.contains(&k.as_str())) {
        return Err(PreregError::UnknownKey { key: key.clone() });
    }
    let unfilled: Vec<String> = OPERATOR_KEYS
        .iter()
        .filter(|key| table.get(**key).and_then(toml::Value::as_str) == Some(PLACEHOLDER))
        .map(|key| (*key).to_string())
        .collect();
    if !unfilled.is_empty() {
        return Err(PreregError::Unfilled { keys: unfilled });
    }

    let memory_cap_bytes = match table.get("memory_cap_bytes") {
        Some(toml::Value::Integer(i)) if *i > 0 => *i as u64,
        Some(other) => {
            return Err(invalid(
                "memory_cap_bytes",
                format!("expected a byte count above 0, found {other}"),
            ));
        }
        None => {
            return Err(PreregError::Missing {
                key: "memory_cap_bytes".to_string(),
            });
        }
    };

    let failures = match table.get("failures") {
        Some(toml::Value::Array(items)) => {
            let mut set = BTreeSet::new();
            for item in items {
                let number = item.as_str().and_then(statement_label).ok_or_else(|| {
                    invalid(
                        "failures",
                        format!("expected labels q1..q{STATEMENT_COUNT}, found {item}"),
                    )
                })?;
                if !set.insert(number) {
                    return Err(invalid("failures", format!("q{number} is listed twice")));
                }
            }
            set
        }
        Some(other) => {
            return Err(invalid(
                "failures",
                format!("expected an array, found {other}"),
            ));
        }
        None => {
            return Err(PreregError::Missing {
                key: "failures".to_string(),
            });
        }
    };

    Ok(Prereg {
        memory_cap_bytes,
        arm_b_hot_s: positive_number(&table, "arm_b_hot_s")?,
        arm_b_cold_s: positive_number(&table, "arm_b_cold_s")?,
        failures,
        rlog_hot_ceiling_s: positive_number(&table, "rlog_hot_ceiling_s")?,
        concurrency_qps_floor: positive_number(&table, "concurrency_qps_floor")?,
    })
}

/// Reads and parses `prereg.toml` from `path`.
pub fn load_prereg(path: &Path) -> Result<Prereg, PreregError> {
    let text = std::fs::read_to_string(path).map_err(|e| PreregError::Read {
        path: path.display().to_string(),
        error: e.to_string(),
    })?;
    parse_prereg(&text)
}

/// The verdict each statement must reach when it answers: the
/// `CardinalityOnly` reason `suite.toml` declares for it, `CardinalityOnly`
/// with no reason for a statement with a LIMIT and no ORDER BY (the
/// comparator cannot identify its rows, Q18), and `Pass` for every other.
pub fn declared_verdicts(suite: &Suite) -> Result<BTreeMap<u32, VerdictRecord>, String> {
    let mut declared = BTreeMap::new();
    for statement in &suite.statements {
        let number = statement.number;
        let over = suite.override_for(number);
        let verdict = if let Some(over) = over.filter(|o| o.is_cardinality_only()) {
            VerdictRecord::CardinalityOnly {
                reason: over.reason.clone(),
            }
        } else if over.is_some_and(|o| {
            o.order_key.as_ref().is_some_and(|k| !k.is_empty())
                || o.order_key_columns.as_ref().is_some_and(|k| !k.is_empty())
        }) {
            VerdictRecord::Pass
        } else {
            let tie = resolve_tie_spec(number, &statement.sql, None, None)
                .map_err(|e| format!("Q{number}: {e}"))?;
            if tie.limit.is_some() && tie.key.is_empty() {
                VerdictRecord::CardinalityOnly { reason: None }
            } else {
                VerdictRecord::Pass
            }
        };
        declared.insert(number, verdict);
    }
    Ok(declared)
}

/// One way a report misses ADR-2040 D7's bar.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Violation {
    #[error("q{number} appears {found} times in the report; it must appear exactly once")]
    StatementNotExactlyOnce { number: u32, found: usize },
    #[error("statement number {number} is not one of q1..q43")]
    UnknownStatement { number: u32 },
    #[error(
        "server setting {setting} appears {found} times in the server log, plus {unreadable} \
         unreadable lines; it must appear exactly once"
    )]
    StampNotExactlyOnce {
        setting: String,
        found: usize,
        unreadable: usize,
    },
    #[error(
        "--{setting} is {command_line} on the bench command line but {server_log} in the server log"
    )]
    StampDiffersFromCommandLine {
        setting: String,
        command_line: u64,
        server_log: u64,
    },
    #[error(
        "the server ran with sql_max_query_bytes {server_log}, but the failures are \
         pre-registered against memory_cap_bytes {prereg}"
    )]
    MemoryCapDiffersFromPrereg { prereg: u64, server_log: u64 },
    #[error("q{number} failed and is not a pre-registered failure: {error}")]
    UnregisteredFailure { number: u32, error: String },
    #[error("q{number} answered with verdict {found}; the accepted verdict is {accepted}")]
    VerdictNotAccepted {
        number: u32,
        found: String,
        accepted: VerdictRecord,
    },
    #[error("hot sum {hot_sum_s} s exceeds 1.25 x arm B's hot, {limit_s} s")]
    HotOverArmB { hot_sum_s: f64, limit_s: f64 },
    #[error("hot sum {hot_sum_s} s is not under the RLOG entry's {ceiling_s} s")]
    HotNotUnderRlog { hot_sum_s: f64, ceiling_s: f64 },
    #[error("cold sum {cold_sum_s} s exceeds 1.25 x arm B's cold, {limit_s} s")]
    ColdOverArmB { cold_sum_s: f64, limit_s: f64 },
    #[error("concurrency phase ran {qps} queries per second, under the floor {floor}")]
    ConcurrencyQpsBelowFloor { qps: f64, floor: f64 },
    #[error("q{number} errored {errors} times in the concurrency phase and is not pre-registered")]
    ConcurrencyUnregisteredError { number: u32, errors: u64 },
    #[error("the concurrency phase failed after it started: {error}")]
    ConcurrencyPhaseFailed { error: String },
    #[error("the declared verdicts could not be derived from suite.toml: {error}")]
    DeclaredVerdictsUnavailable { error: String },
}

/// The factor D7 allows over arm B, hot and cold.
pub const ARM_B_FACTOR: f64 = 1.25;

/// Judges `report` against ADR-2040 D7 and `prereg`. Every rule is checked
/// and every violation returned, not only the first.
pub fn check(report: &ClickBenchParquetReport, prereg: &Prereg) -> Result<(), Vec<Violation>> {
    let declared = suite::load_default()
        .map_err(|e| e.to_string())
        .and_then(|suite| declared_verdicts(&suite));
    check_against(report, prereg, declared)
}

fn check_against(
    report: &ClickBenchParquetReport,
    prereg: &Prereg,
    declared: Result<BTreeMap<u32, VerdictRecord>, String>,
) -> Result<(), Vec<Violation>> {
    let mut violations = Vec::new();

    for number in 1..=STATEMENT_COUNT as u32 {
        let found = report
            .statements
            .iter()
            .filter(|s| s.number == number)
            .count();
        if found != 1 {
            violations.push(Violation::StatementNotExactlyOnce { number, found });
        }
    }
    let unknown: BTreeSet<u32> = report
        .statements
        .iter()
        .map(|s| s.number)
        .filter(|n| !(1..=STATEMENT_COUNT as u32).contains(n))
        .collect();
    violations.extend(
        unknown
            .into_iter()
            .map(|number| Violation::UnknownStatement { number }),
    );

    let provenance = &report.provenance;
    for name in STAMPED_SETTINGS {
        let entry = provenance
            .server_settings
            .iter()
            .filter(|s| s.setting == name)
            .collect::<Vec<_>>();
        let (found, unreadable) = entry.iter().fold((0, 0), |(f, u), s| {
            (f + s.values.len(), u + s.unreadable_lines.len())
        });
        if entry.len() != 1 || found != 1 || unreadable != 0 {
            violations.push(Violation::StampNotExactlyOnce {
                setting: name.to_string(),
                found,
                unreadable,
            });
        }
    }
    let logged = |name: &str| {
        let mut matching = provenance
            .server_settings
            .iter()
            .filter(|s| s.setting == name);
        match (matching.next(), matching.next()) {
            (Some(only), None) => only.single(),
            _ => None,
        }
    };
    for (name, command_line) in [
        ("sql_max_query_bytes", provenance.sql_max_query_bytes),
        ("sql_tenant_max_bytes", provenance.sql_tenant_max_bytes),
    ] {
        if let Some(server_log) = logged(name)
            && server_log != command_line
        {
            violations.push(Violation::StampDiffersFromCommandLine {
                setting: name.replace('_', "-"),
                command_line,
                server_log,
            });
        }
    }
    if let Some(server_log) = logged("sql_max_query_bytes")
        && server_log != prereg.memory_cap_bytes
    {
        violations.push(Violation::MemoryCapDiffersFromPrereg {
            prereg: prereg.memory_cap_bytes,
            server_log,
        });
    }

    match &declared {
        Ok(declared) => {
            for statement in &report.statements {
                if let Some(error) = &statement.error {
                    if !prereg.failures.contains(&statement.number) {
                        violations.push(Violation::UnregisteredFailure {
                            number: statement.number,
                            error: error.clone(),
                        });
                    }
                    continue;
                }
                let Some(accepted) = declared.get(&statement.number) else {
                    continue;
                };
                let accepted_verdict = match (&statement.verdict, accepted) {
                    (Some(VerdictRecord::Pass), _) => true,
                    (Some(found), accepted) => found == accepted,
                    (None, _) => false,
                };
                if !accepted_verdict {
                    violations.push(Violation::VerdictNotAccepted {
                        number: statement.number,
                        found: match (&statement.verdict, &statement.compare_error) {
                            (Some(verdict), _) => verdict.to_string(),
                            (None, Some(error)) => format!("none (comparison failed: {error})"),
                            (None, None) => "none".to_string(),
                        },
                        accepted: accepted.clone(),
                    });
                }
            }
        }
        Err(error) => violations.push(Violation::DeclaredVerdictsUnavailable {
            error: error.clone(),
        }),
    }

    let totals = Totals::from_statements(&report.statements);
    let hot_limit = ARM_B_FACTOR * prereg.arm_b_hot_s;
    if totals.hot_sum_s > hot_limit {
        violations.push(Violation::HotOverArmB {
            hot_sum_s: totals.hot_sum_s,
            limit_s: hot_limit,
        });
    }
    if totals.hot_sum_s >= prereg.rlog_hot_ceiling_s {
        violations.push(Violation::HotNotUnderRlog {
            hot_sum_s: totals.hot_sum_s,
            ceiling_s: prereg.rlog_hot_ceiling_s,
        });
    }
    let cold_limit = ARM_B_FACTOR * prereg.arm_b_cold_s;
    if totals.cold_sum_s > cold_limit {
        violations.push(Violation::ColdOverArmB {
            cold_sum_s: totals.cold_sum_s,
            limit_s: cold_limit,
        });
    }

    if let Some(concurrency) = &report.concurrency {
        if concurrency.qps < prereg.concurrency_qps_floor {
            violations.push(Violation::ConcurrencyQpsBelowFloor {
                qps: concurrency.qps,
                floor: prereg.concurrency_qps_floor,
            });
        }
        // No error-ratio ceiling: one error outside `failures` already fails
        // the phase here, which is stricter than any ceiling over those
        // statements (issue #2055).
        for statement in &concurrency.statements {
            if statement.errors > 0 && !prereg.failures.contains(&statement.number) {
                violations.push(Violation::ConcurrencyUnregisteredError {
                    number: statement.number,
                    errors: statement.errors,
                });
            }
        }
    }
    if let Some(error) = &report.concurrency_error {
        violations.push(Violation::ConcurrencyPhaseFailed {
            error: error.clone(),
        });
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

/// One reference row: each column name with its value's exact JSON text, in
/// the order the row object carried them.
pub type ReferenceRow = Vec<(String, Box<RawValue>)>;

/// One result row of datafusion-cli's `--format json` output: a JSON object
/// whose keys keep their text order, and whose values keep their exact text
/// (so a float's digits reach the comparator unrounded).
struct OrderedRow(ReferenceRow);

impl<'de> Deserialize<'de> for OrderedRow {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowVisitor;
        impl<'de> Visitor<'de> for RowVisitor {
            type Value = OrderedRow;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object holding one result row")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<OrderedRow, A::Error> {
                let mut cells = Vec::new();
                while let Some(entry) = map.next_entry::<String, Box<RawValue>>()? {
                    cells.push(entry);
                }
                Ok(OrderedRow(cells))
            }
        }
        deserializer.deserialize_map(RowVisitor)
    }
}

/// The result rows in a reference file written by `make-reference.sh`.
///
/// datafusion-cli runs `create.sql` and the statement in one process, so the
/// file may hold a document per statement; the query's result is the last.
/// An empty file, or one whose last document is `[]`, is zero rows. A key
/// absent from a row object is a null cell, which is how arrow's JSON writer
/// prints a null by default.
pub fn parse_reference_output(text: &str) -> Result<Vec<ReferenceRow>, String> {
    let mut last = None;
    for document in serde_json::Deserializer::from_str(text).into_iter::<Vec<OrderedRow>>() {
        last = Some(document.map_err(|e| format!("reference is not datafusion-cli JSON: {e}"))?);
    }
    Ok(last
        .unwrap_or_default()
        .into_iter()
        .map(|row| row.0)
        .collect())
}

fn column_names(batches: &[RecordBatch]) -> Option<Vec<String>> {
    batches.first().map(|b| {
        b.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    })
}

/// Judges Ravel's answer to `statement` against a datafusion-cli reference
/// (`parse_reference_output`'s input), under `suite.toml`'s override for it.
///
/// Reference cells are placed in Ravel's column order by column name, and a
/// key missing from a reference row is a null. Unless the override declares
/// `column_match = "by-name"`, the reference's keys must also appear in
/// Ravel's column order, so a positional statement still fails on a column
/// order difference.
pub fn judge_against_reference(
    statement: &Statement,
    over: Option<&StatementOverride>,
    reference_output: &str,
    subject: &[RecordBatch],
) -> Result<ComparisonReport, String> {
    let number = statement.number;
    let reference = parse_reference_output(reference_output)?;
    let (columns, kinds, subject_rows) = match column_names(subject) {
        Some(columns) => {
            let kinds = subject
                .first()
                .map(comparator::schema_kinds)
                .transpose()
                .map_err(|e| format!("ravel schema: {e}"))?
                .unwrap_or_default();
            let rows =
                comparator::rows_from_arrow(subject).map_err(|e| format!("ravel rows: {e}"))?;
            (columns, kinds, rows)
        }
        None if reference.is_empty() => (Vec::new(), Vec::new(), Vec::new()),
        None => {
            return Err(format!(
                "ravel returned no batches; the reference holds {} rows",
                reference.len()
            ));
        }
    };
    for (i, name) in columns.iter().enumerate() {
        if columns[..i].contains(name) {
            return Err(format!("ravel returns column {name:?} twice"));
        }
    }
    let by_name = over.is_some_and(|o| matches!(o.column_match(), ColumnMatch::ByName));
    let mut aligned = Vec::with_capacity(reference.len());
    for (row_index, row) in reference.iter().enumerate() {
        let mut cells = vec!["null"; columns.len()];
        let mut seen = vec![false; columns.len()];
        let mut previous: Option<usize> = None;
        for (key, value) in row {
            let index = columns.iter().position(|c| c == key).ok_or_else(|| {
                format!("reference row {row_index} has column {key:?}, which ravel does not return")
            })?;
            if std::mem::replace(&mut seen[index], true) {
                return Err(format!(
                    "reference row {row_index} carries column {key:?} twice"
                ));
            }
            if !by_name && previous.is_some_and(|p| p > index) {
                return Err(format!(
                    "reference row {row_index} orders column {key:?} differently from ravel"
                ));
            }
            previous = Some(index);
            cells[index] = value.get();
        }
        aligned.push(format!("[{}]", cells.join(",")));
    }
    let reference_rows = comparator::rows_from_json(&format!("[{}]", aligned.join(",")), &kinds)
        .map_err(|e| format!("reference rows: {e}"))?;

    let sql = &statement.sql;
    let tie = if let Some(over) = over.filter(|o| o.is_cardinality_only()) {
        resolve_tie_spec(number, sql, None, over.reason.as_deref())
    } else if let Some(names) = over.and_then(|o| o.order_key_columns.as_deref()) {
        // The reference is already in ravel's column order, so the key
        // resolves against that one order on both sides.
        let key = resolve_order_key_columns(names, &columns, &columns)
            .map_err(|e| format!("order_key_columns: {e}"))?;
        resolve_tie_spec(number, sql, Some(&key), None)
    } else {
        resolve_tie_spec(number, sql, over.and_then(|o| o.order_key.as_deref()), None)
    }
    .map_err(|e| format!("tie spec: {e}"))?;
    let tolerance = over.and_then(StatementOverride::float_tolerance);
    comparator::compare(&reference_rows, &subject_rows, &tie, tolerance.as_ref())
        .map_err(|e| format!("comparator: {e}"))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;
    use crate::clickbench_parquet::concurrency::ConcurrencyStatement;

    const CHECKED_IN_PREREG: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/clickbench/parquet/prereg.toml"
    ));

    const CAP: u64 = 16_451_897_344;
    const TENANT_MAX: u64 = 32_903_794_688;

    fn filled_prereg_text() -> String {
        let mut text = CHECKED_IN_PREREG.to_string();
        for (key, value) in [
            ("memory_cap_bytes", CAP.to_string()),
            ("arm_b_hot_s", "57.5".to_string()),
            ("arm_b_cold_s", "251.7".to_string()),
        ] {
            let placeholder_line = format!("{key} = \"{PLACEHOLDER}\"");
            assert!(
                text.contains(&placeholder_line),
                "prereg.toml has no line {placeholder_line:?}"
            );
            text = text.replace(&placeholder_line, &format!("{key} = {value}"));
        }
        text
    }

    fn prereg() -> Prereg {
        parse_prereg(&filled_prereg_text()).expect("a filled prereg loads")
    }

    fn settings_log() -> String {
        // The shape the default tracing fmt layer writes, ANSI colour on, as
        // a server started with stdout redirected to a file logs it.
        let line = |setting: &str, value: u64| {
            format!(
                "\u{1b}[2m2026-10-03T10:00:00.000000Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \
                 \u{1b}[2mravel_server::config\u{1b}[0m\u{1b}[2m:\u{1b}[0m performance default \
                 resolved \u{1b}[3msetting\u{1b}[0m\u{1b}[2m=\u{1b}[0m\"{setting}\" \
                 \u{1b}[3mvalue\u{1b}[0m\u{1b}[2m=\u{1b}[0m{value} \
                 \u{1b}[3msource\u{1b}[0m\u{1b}[2m=\u{1b}[0m\"derived\"\n"
            )
        };
        [
            line("fetch_concurrency", 32),
            line("store_get_concurrency", 32),
            line("cache_max_bytes", 7_689_077_760),
            line("catalog_cache_max_bytes", 1_537_815_552),
            line("sql_max_query_bytes", CAP),
            line("sql_tenant_max_bytes", TENANT_MAX),
        ]
        .concat()
    }

    /// The declared verdict for every statement, from the checked-in suite.
    fn declared() -> BTreeMap<u32, VerdictRecord> {
        declared_verdicts(&suite::load_default().expect("suite loads")).expect("declared verdicts")
    }

    /// An answered statement whose comparison reached `verdict`.
    fn answered(number: u32, tries: [f64; 3], verdict: VerdictRecord) -> StatementFigures {
        StatementFigures {
            verdict: Some(verdict),
            compare_error: None,
            ..StatementFigures::answered(number, tries, Err(String::new()))
        }
    }

    /// A report that meets every rule: the five pre-registered statements
    /// fail, every other statement reaches its declared verdict, hot and
    /// cold sum to 43 s and 86 s, and the concurrency phase clears the
    /// queries-per-second floor with errors from registered q33 only.
    fn clean_report() -> ClickBenchParquetReport {
        let prereg = prereg();
        let declared = declared();
        let statements: Vec<StatementFigures> = (1..=STATEMENT_COUNT as u32)
            .map(|number| {
                if prereg.failures.contains(&number) {
                    StatementFigures::failed(number, "query memory budget exhausted".to_string())
                } else {
                    answered(number, [2.0, 1.5, 1.0], declared[&number].clone())
                }
            })
            .collect();
        let totals = Totals::from_statements(&statements);
        ClickBenchParquetReport {
            provenance: Provenance {
                git_sha: "0123abcd".to_string(),
                binary: "/opt/ravel/clickbench_parquet_bench".to_string(),
                arm: Arm::A,
                location: "s3://clickbench/hits/".to_string(),
                server: "http://127.0.0.1:4318".to_string(),
                server_log: "/tmp/server.log".to_string(),
                server_settings: parse_server_settings(&settings_log()),
                sql_max_query_bytes: CAP,
                sql_tenant_max_bytes: TENANT_MAX,
                reference_dir: "benchmarks/clickbench/parquet/ref".to_string(),
                reference_version: "datafusion-cli 54.1.0".to_string(),
                ddl_files: Some(100),
            },
            statements,
            totals,
            registered_failures_answered: Vec::new(),
            concurrency: Some(ConcurrencyFigures {
                tasks: 10,
                duration_s: 600.0,
                elapsed_s: 600.0,
                queries_completed: 600,
                errors: 6,
                qps: 1.0,
                error_ratio: 0.01,
                statements: (1..=STATEMENT_COUNT as u32)
                    .map(|number| ConcurrencyStatement {
                        number,
                        completed: 14,
                        errors: u64::from(number == 33) * 6,
                        p50_s: Some(1.0),
                        p95_s: Some(2.0),
                        first_error: (number == 33).then(|| "budget".to_string()),
                    })
                    .collect(),
                errored_statements: vec![33],
            }),
            concurrency_error: None,
        }
    }

    fn only_violation(report: &ClickBenchParquetReport) -> Violation {
        let violations = check(report, &prereg()).expect_err("the report violates one rule");
        assert_eq!(violations.len(), 1, "{violations:#?}");
        violations.into_iter().next().expect("one violation")
    }

    fn statement_mut(report: &mut ClickBenchParquetReport, number: u32) -> &mut StatementFigures {
        report
            .statements
            .iter_mut()
            .find(|s| s.number == number)
            .expect("statement present")
    }

    #[test]
    fn a_clean_report_passes() {
        let report = clean_report();
        assert_eq!(report.totals.hot_sum_s, 38.0);
        assert_eq!(report.totals.cold_sum_s, 76.0);
        assert_eq!(check(&report, &prereg()), Ok(()));
    }

    #[test]
    fn a_missing_statement_is_named() {
        let mut report = clean_report();
        report.statements.retain(|s| s.number != 7);
        assert_eq!(
            only_violation(&report),
            Violation::StatementNotExactlyOnce {
                number: 7,
                found: 0
            }
        );
    }

    #[test]
    fn a_duplicated_statement_is_named() {
        let mut report = clean_report();
        let copy = statement_mut(&mut report, 7).clone();
        report.statements.push(StatementFigures {
            cold_s: Some(0.0),
            hot_s: Some(0.0),
            ..copy
        });
        assert_eq!(
            only_violation(&report),
            Violation::StatementNotExactlyOnce {
                number: 7,
                found: 2
            }
        );
    }

    #[test]
    fn a_statement_outside_the_suite_is_named() {
        let mut report = clean_report();
        report
            .statements
            .push(answered(44, [0.0, 0.0, 0.0], VerdictRecord::Pass));
        assert_eq!(
            only_violation(&report),
            Violation::UnknownStatement { number: 44 }
        );
    }

    #[test]
    fn a_stamp_logged_twice_is_named() {
        let mut report = clean_report();
        let log = format!(
            "{}{}",
            settings_log(),
            "INFO ravel_server::config: performance default resolved \
             setting=\"cache_max_bytes\" value=1 source=\"flag\"\n"
        );
        report.provenance.server_settings = parse_server_settings(&log);
        assert_eq!(
            only_violation(&report),
            Violation::StampNotExactlyOnce {
                setting: "cache_max_bytes".to_string(),
                found: 2,
                unreadable: 0
            }
        );
    }

    #[test]
    fn a_stamp_missing_from_the_log_is_named() {
        let mut report = clean_report();
        let log: String = settings_log()
            .lines()
            .filter(|l| !l.contains("\"catalog_cache_max_bytes\""))
            .map(|l| format!("{l}\n"))
            .collect();
        report.provenance.server_settings = parse_server_settings(&log);
        assert_eq!(
            only_violation(&report),
            Violation::StampNotExactlyOnce {
                setting: "catalog_cache_max_bytes".to_string(),
                found: 0,
                unreadable: 0
            }
        );
    }

    #[test]
    fn a_command_line_budget_the_server_did_not_run_is_named() {
        let mut report = clean_report();
        report.provenance.sql_tenant_max_bytes = TENANT_MAX - 1;
        assert_eq!(
            only_violation(&report),
            Violation::StampDiffersFromCommandLine {
                setting: "sql-tenant-max-bytes".to_string(),
                command_line: TENANT_MAX - 1,
                server_log: TENANT_MAX,
            }
        );
    }

    #[test]
    fn a_server_cap_other_than_the_registered_one_is_named() {
        let report = clean_report();
        let mut other = prereg();
        other.memory_cap_bytes = CAP + 1;
        let violations = check(&report, &other).expect_err("the cap differs");
        assert_eq!(
            violations,
            vec![Violation::MemoryCapDiffersFromPrereg {
                prereg: CAP + 1,
                server_log: CAP
            }]
        );
    }

    #[test]
    fn a_failure_outside_the_registered_set_is_named() {
        let mut report = clean_report();
        *statement_mut(&mut report, 5) = StatementFigures::failed(5, "boom".to_string());
        assert_eq!(
            only_violation(&report),
            Violation::UnregisteredFailure {
                number: 5,
                error: "boom".to_string()
            }
        );
    }

    #[test]
    fn a_registered_failure_that_answers_is_reported_not_a_violation() {
        let mut report = clean_report();
        *statement_mut(&mut report, 29) = answered(29, [0.5, 0.5, 0.5], VerdictRecord::Pass);
        assert_eq!(check(&report, &prereg()), Ok(()));
        assert_eq!(
            registered_failures_answered(&report.statements, &prereg()),
            vec![29]
        );
    }

    #[test]
    fn a_failed_comparison_is_named_with_the_accepted_verdict() {
        let mut report = clean_report();
        statement_mut(&mut report, 5).verdict = Some(VerdictRecord::Fail);
        assert_eq!(
            only_violation(&report),
            Violation::VerdictNotAccepted {
                number: 5,
                found: "Fail".to_string(),
                accepted: VerdictRecord::Pass
            }
        );
    }

    #[test]
    fn a_cardinality_verdict_the_suite_does_not_declare_is_not_accepted() {
        let mut report = clean_report();
        statement_mut(&mut report, 25).verdict =
            Some(VerdictRecord::CardinalityOnly { reason: None });
        let violation = only_violation(&report);
        assert!(
            matches!(
                &violation,
                Violation::VerdictNotAccepted {
                    number: 25,
                    accepted: VerdictRecord::CardinalityOnly { reason: Some(_) },
                    ..
                }
            ),
            "{violation:?}"
        );
    }

    #[test]
    fn hot_over_one_and_a_quarter_arm_b_is_named() {
        let report = clean_report();
        let mut tight = prereg();
        // 1.25 x 30 = 37.5 s, under the clean report's 38 s hot sum.
        tight.arm_b_hot_s = 30.0;
        let violations = check(&report, &tight).expect_err("hot is over");
        assert_eq!(
            violations,
            vec![Violation::HotOverArmB {
                hot_sum_s: 38.0,
                limit_s: 37.5
            }]
        );
    }

    #[test]
    fn hot_at_the_rlog_ceiling_is_named() {
        let report = clean_report();
        let mut tight = prereg();
        tight.rlog_hot_ceiling_s = 38.0;
        let violations = check(&report, &tight).expect_err("hot is not under the ceiling");
        assert_eq!(
            violations,
            vec![Violation::HotNotUnderRlog {
                hot_sum_s: 38.0,
                ceiling_s: 38.0
            }]
        );
    }

    #[test]
    fn cold_over_one_and_a_quarter_arm_b_is_named() {
        let report = clean_report();
        let mut tight = prereg();
        // 1.25 x 60 = 75 s, under the clean report's 76 s cold sum.
        tight.arm_b_cold_s = 60.0;
        let violations = check(&report, &tight).expect_err("cold is over");
        assert_eq!(
            violations,
            vec![Violation::ColdOverArmB {
                cold_sum_s: 76.0,
                limit_s: 75.0
            }]
        );
    }

    #[test]
    fn concurrency_under_the_qps_floor_is_named() {
        let mut report = clean_report();
        report.concurrency.as_mut().expect("ran").qps = 0.399;
        assert_eq!(
            only_violation(&report),
            Violation::ConcurrencyQpsBelowFloor {
                qps: 0.399,
                floor: 0.400
            }
        );
    }

    /// A phase in which every statement ran 14 times and the five
    /// pre-registered failures errored on every run, so the raw error ratio
    /// is exactly 5/43.
    fn registered_failures_always_error(report: &mut ClickBenchParquetReport) {
        let failures = prereg().failures;
        let concurrency = report.concurrency.as_mut().expect("ran");
        for statement in &mut concurrency.statements {
            if failures.contains(&statement.number) {
                statement.completed = 0;
                statement.errors = 14;
                statement.p50_s = None;
                statement.p95_s = None;
                statement.first_error = Some("budget".to_string());
            } else {
                statement.completed = 14;
                statement.errors = 0;
                statement.first_error = None;
            }
        }
        concurrency.queries_completed = 38 * 14;
        concurrency.errors = 5 * 14;
        concurrency.error_ratio = 70.0 / 602.0;
        concurrency.errored_statements = failures.iter().copied().collect();
    }

    #[test]
    fn registered_failures_erroring_at_five_in_forty_three_pass() {
        let mut report = clean_report();
        registered_failures_always_error(&mut report);
        let concurrency = report.concurrency.as_ref().expect("ran");
        assert_eq!(
            concurrency.error_ratio.to_bits(),
            (5.0f64 / 43.0).to_bits(),
            "the raw ratio is 5/43"
        );
        assert!(concurrency.error_ratio > RLOG_ERROR_RATIO);
        assert_eq!(check(&report, &prereg()), Ok(()));
    }

    #[test]
    fn a_single_unregistered_concurrency_error_fails_the_phase() {
        // On top of the registered failures, q8 errors once in its 14 runs:
        // one error among the 38 * 14 = 532 runs of unregistered statements.
        let mut report = clean_report();
        registered_failures_always_error(&mut report);
        let concurrency = report.concurrency.as_mut().expect("ran");
        let q8 = concurrency
            .statements
            .iter_mut()
            .find(|s| s.number == 8)
            .expect("q8");
        q8.completed = 13;
        q8.errors = 1;
        q8.first_error = Some("budget".to_string());
        concurrency.queries_completed = 38 * 14 - 1;
        concurrency.errors = 5 * 14 + 1;
        concurrency.error_ratio = 71.0 / 602.0;
        concurrency.errored_statements = vec![8, 19, 29, 33, 34, 35];
        assert_eq!(
            only_violation(&report),
            Violation::ConcurrencyUnregisteredError {
                number: 8,
                errors: 1
            }
        );
    }

    #[test]
    fn a_concurrency_error_outside_the_registered_set_is_named() {
        let mut report = clean_report();
        let concurrency = report.concurrency.as_mut().expect("ran");
        concurrency
            .statements
            .iter_mut()
            .find(|s| s.number == 8)
            .expect("q8")
            .errors = 2;
        concurrency.errored_statements = vec![8, 33];
        assert_eq!(
            only_violation(&report),
            Violation::ConcurrencyUnregisteredError {
                number: 8,
                errors: 2
            }
        );
    }

    #[test]
    fn unavailable_declared_verdicts_are_named() {
        let report = clean_report();
        let violations = check_against(&report, &prereg(), Err("suite.toml unreadable".into()))
            .expect_err("no declared verdicts");
        assert_eq!(
            violations,
            vec![Violation::DeclaredVerdictsUnavailable {
                error: "suite.toml unreadable".to_string()
            }]
        );
    }

    #[test]
    fn no_concurrency_phase_checks_no_concurrency_floor() {
        let mut report = clean_report();
        report.concurrency = None;
        assert_eq!(check(&report, &prereg()), Ok(()));
    }

    #[test]
    fn a_concurrency_phase_that_failed_after_it_started_is_named() {
        let mut report = clean_report();
        report.concurrency = None;
        report.concurrency_error = Some("task 3 panicked".to_string());
        assert_eq!(
            only_violation(&report),
            Violation::ConcurrencyPhaseFailed {
                error: "task 3 panicked".to_string()
            }
        );
    }

    #[test]
    fn the_declared_verdicts_match_the_suite() {
        let declared = declared();
        assert_eq!(declared.len(), STATEMENT_COUNT);
        let suite = suite::load_default().expect("suite loads");
        for (number, verdict) in &declared {
            let expected = match number {
                18 => VerdictRecord::CardinalityOnly { reason: None },
                25 | 27 => VerdictRecord::CardinalityOnly {
                    reason: suite.override_for(*number).and_then(|o| o.reason.clone()),
                },
                _ => VerdictRecord::Pass,
            };
            assert_eq!(verdict, &expected, "Q{number}");
        }
        assert!(matches!(
            declared.get(&25),
            Some(VerdictRecord::CardinalityOnly { reason: Some(_) })
        ));
    }

    #[test]
    fn hot_is_the_better_of_tries_two_and_three_and_cold_is_try_one() {
        let figures = StatementFigures::answered(3, [5.0, 2.0, 3.0], Err(String::new()));
        assert_eq!(figures.cold_s, Some(5.0));
        assert_eq!(figures.hot_s, Some(2.0));
        let figures = StatementFigures::answered(3, [5.0, 4.0, 3.0], Err(String::new()));
        assert_eq!(figures.hot_s, Some(3.0));
    }

    #[test]
    fn server_settings_parse_from_coloured_and_plain_lines() {
        let plain = "2026-10-03T10:00:00Z  INFO ravel_server::config: performance default \
                     resolved setting=\"cache_max_bytes\" value=8053063680 source=\"derived\"\n\
                     2026-10-03T10:00:00Z  INFO ravel_server::config: performance default \
                     resolved setting=\"catalog_cache_max_bytes\" value=x source=\"derived\"\n";
        let settings = parse_server_settings(plain);
        let cache = &settings[0];
        assert_eq!(cache.setting, "cache_max_bytes");
        assert_eq!(cache.values, vec![8_053_063_680]);
        // `catalog_cache_max_bytes` is not read as `cache_max_bytes`.
        assert_eq!(settings[1].setting, "catalog_cache_max_bytes");
        assert!(settings[1].values.is_empty());
        assert_eq!(settings[1].unreadable_lines.len(), 1);

        let coloured = parse_server_settings(&settings_log());
        let values: Vec<Option<u64>> = coloured.iter().map(ServerSetting::single).collect();
        assert_eq!(
            values,
            vec![
                Some(7_689_077_760),
                Some(1_537_815_552),
                Some(32),
                Some(CAP),
                Some(TENANT_MAX)
            ]
        );
    }

    #[test]
    fn the_checked_in_prereg_refuses_each_unfilled_key() {
        assert_eq!(
            parse_prereg(CHECKED_IN_PREREG),
            Err(PreregError::Unfilled {
                keys: vec![
                    "memory_cap_bytes".to_string(),
                    "arm_b_hot_s".to_string(),
                    "arm_b_cold_s".to_string()
                ]
            })
        );
    }

    #[test]
    fn a_filled_copy_of_the_checked_in_prereg_loads() {
        assert_eq!(
            prereg(),
            Prereg {
                memory_cap_bytes: CAP,
                arm_b_hot_s: 57.5,
                arm_b_cold_s: 251.7,
                failures: BTreeSet::from([19, 29, 33, 34, 35]),
                rlog_hot_ceiling_s: 101.7,
                concurrency_qps_floor: 0.400,
            }
        );
    }

    #[test]
    fn a_prereg_still_carrying_the_error_ratio_ceiling_is_refused() {
        let line = "concurrency_error_ratio_ceiling = 0.101\n";
        for text in [
            format!("{}\n{line}", filled_prereg_text()),
            format!("{CHECKED_IN_PREREG}\n{line}"),
        ] {
            let error = parse_prereg(&text).expect_err("a removed key");
            assert!(
                matches!(
                    &error,
                    PreregError::RemovedKey { key, .. } if key == "concurrency_error_ratio_ceiling"
                ),
                "{error:?}"
            );
            let message = error.to_string();
            assert!(
                message.contains("concurrency_error_ratio_ceiling was removed (issue #2055)"),
                "{message}"
            );
        }
    }

    #[test]
    fn a_zero_or_partly_filled_prereg_is_refused() {
        let zero = filled_prereg_text().replace("arm_b_hot_s = 57.5", "arm_b_hot_s = 0");
        assert!(matches!(
            parse_prereg(&zero),
            Err(PreregError::Invalid { key, .. }) if key == "arm_b_hot_s"
        ));
        let partly = CHECKED_IN_PREREG.replace(
            &format!("memory_cap_bytes = \"{PLACEHOLDER}\""),
            "memory_cap_bytes = 1",
        );
        assert_eq!(
            parse_prereg(&partly),
            Err(PreregError::Unfilled {
                keys: vec!["arm_b_hot_s".to_string(), "arm_b_cold_s".to_string()]
            })
        );
        let missing = filled_prereg_text().replace("arm_b_cold_s = 251.7", "");
        assert_eq!(
            parse_prereg(&missing),
            Err(PreregError::Missing {
                key: "arm_b_cold_s".to_string()
            })
        );
        let unknown = format!("{}\narm_b_hot = 1.0\n", filled_prereg_text());
        assert_eq!(
            parse_prereg(&unknown),
            Err(PreregError::UnknownKey {
                key: "arm_b_hot".to_string()
            })
        );
        let bad_label = filled_prereg_text().replace("\"q19\"", "\"q44\"");
        assert!(matches!(
            parse_prereg(&bad_label),
            Err(PreregError::Invalid { key, .. }) if key == "failures"
        ));
    }

    fn batch(columns: Vec<(&str, Arc<dyn datafusion::arrow::array::Array>)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, columns.into_iter().map(|(_, a)| a).collect()).expect("batch")
    }

    fn statement(number: u32, sql: &str) -> Statement {
        Statement {
            number,
            sql: sql.to_string(),
        }
    }

    #[test]
    fn a_reference_with_a_ddl_document_and_an_omitted_null_matches() {
        let subject = vec![batch(vec![
            (
                "k",
                Arc::new(StringArray::from(vec![Some("a"), None])) as Arc<_>,
            ),
            ("c", Arc::new(Int64Array::from(vec![2, 1])) as Arc<_>),
            ("f", Arc::new(Float64Array::from(vec![0.1, 0.25])) as Arc<_>),
        ])];
        // datafusion-cli's output for create.sql (an empty array) followed by
        // the statement's; the second row's null `k` is left out.
        let reference = "[]\n[{\"k\":\"a\",\"c\":2,\"f\":0.1},{\"c\":1,\"f\":0.25}]\n";
        let report = judge_against_reference(
            &statement(
                1,
                "SELECT k, COUNT(*) AS c, AVG(x) AS f FROM hits GROUP BY k",
            ),
            None,
            reference,
            &subject,
        )
        .expect("comparison runs");
        assert_eq!(report.verdict, Verdict::Pass);

        let wrong = "[{\"k\":\"a\",\"c\":2,\"f\":0.1},{\"c\":1,\"f\":0.5}]";
        let report = judge_against_reference(
            &statement(
                1,
                "SELECT k, COUNT(*) AS c, AVG(x) AS f FROM hits GROUP BY k",
            ),
            None,
            wrong,
            &subject,
        )
        .expect("comparison runs");
        assert_eq!(report.verdict, Verdict::Fail);
    }

    #[test]
    fn a_reference_in_another_column_order_is_refused_unless_by_name() {
        let subject = vec![batch(vec![
            ("a", Arc::new(Int64Array::from(vec![1])) as Arc<_>),
            ("b", Arc::new(Int64Array::from(vec![2])) as Arc<_>),
        ])];
        let reference = "[{\"b\":2,\"a\":1}]";
        let st = statement(24, "SELECT a, b FROM hits");
        let error = judge_against_reference(&st, None, reference, &subject)
            .expect_err("positional statement");
        assert!(error.contains("orders column"), "{error}");
        let over = StatementOverride {
            number: 24,
            order_key: None,
            order_key_columns: None,
            compare: None,
            column_match: Some("by-name".to_string()),
            reason: Some("view column order".to_string()),
            ci_expected_error: None,
            float_reason: None,
            float_max_ulps: None,
        };
        let report = judge_against_reference(&st, Some(&over), reference, &subject)
            .expect("by-name comparison runs");
        assert_eq!(report.verdict, Verdict::Pass);
    }

    #[test]
    fn an_empty_reference_against_no_batches_passes() {
        let report = judge_against_reference(
            &statement(28, "SELECT a FROM hits GROUP BY a HAVING COUNT(*) > 100000"),
            None,
            "",
            &[],
        )
        .expect("comparison runs");
        assert_eq!(report.verdict, Verdict::Pass);
        let error = judge_against_reference(
            &statement(28, "SELECT a FROM hits"),
            None,
            "[{\"a\":1}]",
            &[],
        )
        .expect_err("rows against no batches");
        assert!(error.contains("no batches"), "{error}");
    }

    #[test]
    fn arm_locations_and_file_counts() {
        assert_eq!(Arm::A.check_location("s3://clickbench/hits/"), Ok(()));
        assert!(
            Arm::A
                .check_location("s3://clickbench/hits.parquet")
                .is_err()
        );
        assert_eq!(
            Arm::B.check_location("s3://clickbench/hits.parquet"),
            Ok(())
        );
        assert!(Arm::B.check_location("s3://clickbench/hits/").is_err());
        assert_eq!(Arm::A.expected_files(), 100);
        assert_eq!(Arm::B.expected_files(), 1);
    }

    #[test]
    fn a_report_round_trips_through_json() {
        let report = clean_report();
        let text = serde_json::to_string(&report).expect("serialises");
        let back: ClickBenchParquetReport = serde_json::from_str(&text).expect("parses");
        assert_eq!(back, report);
    }
}
