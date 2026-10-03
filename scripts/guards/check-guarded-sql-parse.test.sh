#!/usr/bin/env bash
# Cases for check-guarded-sql-parse.sh. Add a case here before changing a rule,
# the way check-test-hygiene.test.sh works for the hygiene guard.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout.
#
# Run: bash scripts/guards/check-guarded-sql-parse.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-guarded-sql-parse.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-guarded-sql-parse-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed and a valid anchor.
# Prints its path. Cases that need a broken anchor overwrite the file.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/crates/ravel-sql/src"
  cp "${GUARD}" "${dir}/scripts/guards/check-guarded-sql-parse.sh"
  cat >"${dir}/crates/ravel-sql/src/complexity_guard.rs" <<'RS'
//! The guard module. Its prose names DFParser and DFParserBuilder freely.
pub fn check(sql: &str) -> Result<(), StatementTooComplex> {
    Ok(())
}

pub(crate) fn parse_guarded(sql: &str) -> Result<Parsed, GuardedParseError> {
    check(sql)?;
    datafusion::sql::parser::DFParserBuilder::new(sql)
        .with_recursion_limit(PARSER_RECURSION_LIMIT)
        .build()
        .and_then(|mut parser| parser.parse_statements())
        .map_err(|e| GuardedParseError::Parse(e.to_string()))
}
RS
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-guarded-sql-parse.sh 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- the clean shape -------------------------------------------------------

d="$(new_repo clean)"
cat >"${d}/crates/ravel-sql/src/validate.rs" <<'RS'
//! Parsing here uses datafusion::sql::parser::DFParser, not bare sqlparser.
pub fn validate(sql: &str) -> Result<(), ValidationError> {
    let statements = complexity_guard::parse_guarded(sql)?;
    Ok(())
}
RS
check "passes when every parse goes through the anchor" "${d}" 0 "every SQL parse goes through"

# --- findings --------------------------------------------------------------

d="$(new_repo bare_builder)"
cat >"${d}/crates/ravel-sql/src/page_plan.rs" <<'RS'
fn parse_query(sql: &str) -> Result<Query, PagePlanError> {
    let statements = DFParserBuilder::new(sql)
        .with_recursion_limit(50)
        .build()
        .and_then(|mut parser| parser.parse_statements())?;
    Ok(statements)
}
RS
check "flags a bare DFParserBuilder in a new function" "${d}" 1 "page_plan.rs:2: bare-parse:"

d="$(new_repo bare_import)"
cat >"${d}/crates/ravel-sql/src/audit.rs" <<'RS'
use datafusion::sql::parser::DFParser;
RS
check "flags a bare DFParser import" "${d}" 1 "audit.rs:1: bare-parse:"

d="$(new_repo bare_sqlparser)"
cat >"${d}/crates/ravel-sql/src/audit.rs" <<'RS'
fn shape_of(sql: &str) -> Statement {
    Parser::parse_sql(&GenericDialect {}, sql).expect("parsed")
}
RS
check "flags sqlparser's own Parser, not just the DataFusion one" "${d}" 1 "audit.rs:2: bare-parse:"

# --- prose and strings are not code ----------------------------------------

d="$(new_repo prose)"
cat >"${d}/crates/ravel-sql/src/validate.rs" <<'RS'
//! DFParser::parse_sql builds a sqlparser AST, and DFParserBuilder sets the
//! recursion limit. None of that is a call.
/* A block comment naming DFParserBuilder::new, spanning
   two lines, also names Parser::parse_sql. */
fn describe() -> &'static str {
    "DFParser::parse_sql"
}
RS
check "ignores parser names in comments and string literals" "${d}" 0 ""

d="$(new_repo lifetime)"
cat >"${d}/crates/ravel-sql/src/validate.rs" <<'RS'
fn borrow<'a>(sql: &'a str) -> &'a str {
    sql
}
fn parse(sql: &str) -> Statements {
    DFParserBuilder::new(sql).build().unwrap()
}
RS
check "a lifetime tick does not blind the scan to a later finding" "${d}" 1 "validate.rs:5: bare-parse:"

d="$(new_repo multiline_string)"
cat >"${d}/crates/ravel-sql/src/validate.rs" <<'RS'
fn message() -> String {
    format!(
        "the front end is DFParser, built by
         DFParserBuilder, and neither line is code"
    )
}
RS
check "a parser name on the second line of a string is not code" "${d}" 0 ""

# --- the allow marker ------------------------------------------------------

d="$(new_repo marker_inline)"
cat >"${d}/crates/ravel-sql/src/redact.rs" <<'RS'
#[cfg(test)]
mod tests {
    fn reparse(sql: &str) {
        DFParser::parse_sql(sql).expect("out"); // guarded-parse-allow: fixture text
    }
}
RS
check "an inline marker with a reason suppresses" "${d}" 0 ""

d="$(new_repo marker_block)"
cat >"${d}/crates/ravel-sql/src/redact.rs" <<'RS'
#[cfg(test)]
mod tests {
    // guarded-parse-allow: the subject is the redacted output, which must
    // re-parse through the raw front end.
    fn reparse(sql: &str) {
        DFParser::parse_sql(sql).expect("out");
    }
}
RS
check "a marker above the function does not cover a line inside it" "${d}" 1 "redact.rs:6: bare-parse:"

d="$(new_repo marker_adjacent)"
cat >"${d}/crates/ravel-sql/src/redact.rs" <<'RS'
#[cfg(test)]
mod tests {
    fn reparse(sql: &str) {
        // guarded-parse-allow: the subject is the redacted output, which
        // must re-parse through the raw front end.
        DFParser::parse_sql(sql).expect("out");
    }
}
RS
check "a marker in the block directly above the line suppresses" "${d}" 0 ""

d="$(new_repo marker_no_reason)"
cat >"${d}/crates/ravel-sql/src/redact.rs" <<'RS'
fn reparse(sql: &str) {
    // guarded-parse-allow:
    DFParser::parse_sql(sql).expect("out");
}
RS
check "a marker with no reason does not suppress" "${d}" 1 "redact.rs:3: bare-parse:"

d="$(new_repo marker_stale)"
cat >"${d}/crates/ravel-sql/src/redact.rs" <<'RS'
// guarded-parse-allow: this reason belongs to the function below it.
fn documented(sql: &str) {}

fn later(sql: &str) {
    DFParser::parse_sql(sql).expect("out");
}
RS
check "a marker does not carry past its own comment block" "${d}" 1 "redact.rs:5: bare-parse:"

d="$(new_repo marker_in_string)"
cat >"${d}/crates/ravel-sql/src/redact.rs" <<'RS'
fn reparse(sql: &str) {
    DFParser::parse_sql(sql).expect("guarded-parse-allow: not a comment");
}
RS
check "the marker only counts from a comment, not a string" "${d}" 1 "redact.rs:2: bare-parse:"

# --- the anchor ------------------------------------------------------------

d="$(new_repo anchor_renamed)"
cat >"${d}/crates/ravel-sql/src/complexity_guard.rs" <<'RS'
pub(crate) fn parse_checked(sql: &str) -> Result<Parsed, GuardedParseError> {
    check(sql)?;
    datafusion::sql::parser::DFParserBuilder::new(sql).build()
}
RS
check "refuses to run when the anchor function is gone" "${d}" 2 "expected exactly one"

d="$(new_repo anchor_duplicated)"
cat >>"${d}/crates/ravel-sql/src/complexity_guard.rs" <<'RS'

pub(crate) fn parse_guarded(sql: &str) -> Result<Parsed, GuardedParseError> {
    datafusion::sql::parser::DFParserBuilder::new(sql).build()
}
RS
check "refuses when a second anchor function appears" "${d}" 2 "expected exactly one"

d="$(new_repo anchor_no_parser)"
cat >"${d}/crates/ravel-sql/src/complexity_guard.rs" <<'RS'
pub(crate) fn parse_guarded(sql: &str) -> Result<Parsed, GuardedParseError> {
    check(sql)?;
    somebody_elses_parse(sql)
}
RS
check "refuses when the anchor no longer builds a parser" "${d}" 2 "no longer builds a parser"

d="$(new_repo anchor_scope)"
cat >"${d}/crates/ravel-sql/src/validate.rs" <<'RS'
pub(crate) fn parse_guarded(sql: &str) -> Result<Parsed, GuardedParseError> {
    DFParserBuilder::new(sql).build()
}
RS
check "a parse_guarded in another file is still a finding" "${d}" 1 "validate.rs:2: bare-parse:"

d="$(new_repo anchor_body_ends)"
cat >>"${d}/crates/ravel-sql/src/complexity_guard.rs" <<'RS'

fn later_in_the_same_file(sql: &str) -> Parsed {
    DFParserBuilder::new(sql).build()
}
RS
check "the exemption ends with the anchor function body" "${d}" 1 "complexity_guard.rs:16: bare-parse:"

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
out="$(cd "${d}" && bash scripts/guards/check-guarded-sql-parse.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    %s\n' "an unknown option is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "an unknown option is a usage error" "${rc}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-guarded-sql-parse.sh crates/nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no such directory"* ]]; then
  printf 'ok    %s\n' "a missing root is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "a missing root is a usage error" "${rc}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
