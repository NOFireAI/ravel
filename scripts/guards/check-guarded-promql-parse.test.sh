#!/usr/bin/env bash
# Cases for check-guarded-promql-parse.sh, written the way
# check-guarded-sql-parse.test.sh is written for the SQL guard: a case before
# a rule change, never after.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout. Every fixture carries BOTH roots,
# because the whole reason this guard differs from the SQL one is that the
# PromQL parse funnel has a caller in another crate.
#
# Two wrong implementations this file exists to rule out:
#   * a scan that finds nothing and reports clean (no funnel, no sources, a
#     root that is not there): every such case asserts exit 2, never 0;
#   * a scan over crates/ravel-promql/src only, which passes every in-crate
#     case while the cross-crate entry point in crates/ravel-query/src stays
#     unguarded: `flags a bare parse under the second root` and
#     `refuses when the second root routes no parse through the funnel` fail
#     against it.
#
# Run: bash scripts/guards/check-guarded-promql-parse.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-guarded-promql-parse.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-guarded-promql-parse-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed, a valid funnel
# under the first root, and a second root that routes a parse through it.
# Prints its path. Cases that need a broken anchor overwrite the file.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/crates/ravel-promql/src" \
    "${dir}/crates/ravel-query/src"
  cp "${GUARD}" "${dir}/scripts/guards/check-guarded-promql-parse.sh"
  cat >"${dir}/crates/ravel-promql/src/complexity_guard.rs" <<'RS'
//! The guard module. Its prose names promql_parser::parser::parse freely.
pub fn check(query: &str) -> Result<(), QueryTooComplex> {
    Ok(())
}

pub fn parse_guarded(query: &str) -> Result<Expr, GuardedParseError> {
    check(query)?;
    promql_parser::parser::parse(query).map_err(GuardedParseError::Parse)
}
RS
  cat >"${dir}/crates/ravel-query/src/engine.rs" <<'RS'
use promql_parser::parser::{Expr, Offset};

fn parse_selector(query: &str) -> Result<Selector, QueryError> {
    let expr = ravel_promql::complexity_guard::parse_guarded(query)
        .map_err(|e| QueryError::Parse(e.to_string()))?;
    Ok(selector_of(expr))
}
RS
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-guarded-promql-parse.sh 2>&1)" || rc=$?
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
cat >"${d}/crates/ravel-promql/src/eval.rs" <<'RS'
//! Evaluation parses through promql_parser::parser::parse, behind the funnel.
use promql_parser::parser::Expr;

pub fn eval_instant(query: &str) -> Result<Value, Error> {
    let expr = crate::complexity_guard::parse_guarded(query)?;
    eval_expr(&expr)
}
RS
check "passes when every parse goes through the funnel" "${d}" 0 \
  "every PromQL parse goes through"

# --- findings, first root --------------------------------------------------

d="$(new_repo bare_qualified)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
pub fn plan_selectors(query: &str) -> Result<Vec<SelectorPlan>, Error> {
    let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
    Ok(walk(&expr))
}
RS
check "flags a fully qualified promql_parser parse" "${d}" 1 "plan.rs:2: bare-parse:"

d="$(new_repo bare_import)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
use promql_parser::parser::{Expr, parse};
RS
check "flags a use that imports the parse entry point" "${d}" 1 "plan.rs:1: bare-parse:"

d="$(new_repo imported_call)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
use promql_parser::parser::{Expr, parse}; // guarded-parse-allow: the import is
                                          // not the parse; the call below is.
pub fn plan(query: &str) -> Expr {
    parse(query).expect("parses")
}
RS
check "flags a call through an imported parse name" "${d}" 1 "plan.rs:4: bare-parse:"

d="$(new_repo module_alias)"
cat >"${d}/crates/ravel-promql/src/redact.rs" <<'RS'
use promql_parser::parser::{self, Expr};

pub fn redact(query: &str) -> Result<String, RedactError> {
    let mut expr = parser::parse(query).map_err(|_| RedactError::Parse)?;
    Ok(expr.to_string())
}
RS
check "flags parser::parse reached through a self import" "${d}" 1 "redact.rs:4: bare-parse:"

d="$(new_repo function_alias)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
use promql_parser::parser::parse as pparse; // guarded-parse-allow: fixture text

pub fn g(q: &str) -> Expr {
    pparse(q).expect("parses")
}

pub fn g2(q: &str) -> Expr {
    pparse(q).expect("parses")
}
RS
check "flags calls through an ALIASED parse import" "${d}" 1 "plan.rs:4: bare-parse:"

d="$(new_repo multiline_use)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
use promql_parser::parser::{
    Expr,
    parse,
};
RS
check "flags a parse imported by a multi-line use" "${d}" 1 "plan.rs:3: bare-parse:"

d="$(new_repo lexer_entry)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
pub fn tokens(query: &str) -> Vec<Token> {
    promql_parser::parser::lexer(query).collect()
}
RS
check "flags the lexer front end, not just parse" "${d}" 1 "plan.rs:2: bare-parse:"

# --- findings, second root -------------------------------------------------
#
# The case a one-root scan cannot pass: the file routes one parse through the
# funnel (so the second anchor holds) and sneaks a second one past it.

d="$(new_repo cross_crate)"
cat >>"${d}/crates/ravel-query/src/engine.rs" <<'RS'

fn label_values(query: &str) -> Result<Vec<String>, QueryError> {
    let expr = promql_parser::parser::parse(query).map_err(QueryError::Parse)?;
    Ok(names_of(expr))
}
RS
check "flags a bare parse under the second root" "${d}" 1 "engine.rs:10: bare-parse:"

# --- prose and strings are not code ----------------------------------------

d="$(new_repo prose)"
cat >"${d}/crates/ravel-promql/src/eval.rs" <<'RS'
//! promql_parser::parser::parse is a recursive-descent parser, and
//! promql_parser::parser::lexer feeds it. None of that is a call.
/* A block comment naming promql_parser::parser::parse, spanning
   two lines, also names parser::parse. */
fn describe() -> &'static str {
    "promql_parser::parser::parse"
}
RS
check "ignores parse mentions in comments and string literals" "${d}" 0 ""

d="$(new_repo lifetime)"
cat >"${d}/crates/ravel-promql/src/eval.rs" <<'RS'
fn borrow<'a>(query: &'a str) -> &'a str {
    query
}
fn parse_it(query: &str) -> Expr {
    promql_parser::parser::parse(query).expect("parses")
}
RS
check "a lifetime tick does not blind the scan to a later finding" "${d}" 1 \
  "eval.rs:5: bare-parse:"

d="$(new_repo multiline_string)"
cat >"${d}/crates/ravel-promql/src/eval.rs" <<'RS'
fn message() -> String {
    format!(
        "the front end is promql_parser, reached through
         promql_parser::parser::parse, and neither line is code"
    )
}
RS
check "a parse mention on the second line of a string is not code" "${d}" 0 ""

d="$(new_repo unrelated_parse)"
cat >"${d}/crates/ravel-query/src/params.rs" <<'RS'
impl Params {
    pub fn parse(query_string: Option<&str>) -> Self {
        let step: u64 = raw.parse().unwrap_or(0);
        Params { step }
    }
}
RS
check "a parse unrelated to promql_parser is not a finding" "${d}" 0 ""

# --- the allow marker ------------------------------------------------------

d="$(new_repo marker_inline)"
cat >"${d}/crates/ravel-promql/src/matchers.rs" <<'RS'
#[cfg(test)]
mod tests {
    fn fixture() -> Expr {
        promql_parser::parser::parse("up").expect("parses") // guarded-parse-allow: fixture text
    }
}
RS
check "an inline marker with a reason suppresses" "${d}" 0 ""

d="$(new_repo marker_block)"
cat >"${d}/crates/ravel-promql/src/matchers.rs" <<'RS'
#[cfg(test)]
mod tests {
    fn fixture() -> Expr {
        // guarded-parse-allow: the subject is the raw front end's own
        // error message, which the funnel's error type wraps.
        promql_parser::parser::parse("up").expect("parses")
    }
}
RS
check "a marker in the block directly above the line suppresses" "${d}" 0 ""

d="$(new_repo marker_no_reason)"
cat >"${d}/crates/ravel-promql/src/matchers.rs" <<'RS'
fn fixture() -> Expr {
    // guarded-parse-allow:
    promql_parser::parser::parse("up").expect("parses")
}
RS
check "a marker with no reason does not suppress" "${d}" 1 "matchers.rs:3: bare-parse:"

d="$(new_repo marker_stale)"
cat >"${d}/crates/ravel-promql/src/matchers.rs" <<'RS'
// guarded-parse-allow: this reason belongs to the function below it.
fn documented(query: &str) {}

fn later(query: &str) -> Expr {
    promql_parser::parser::parse(query).expect("parses")
}
RS
check "a marker does not carry past its own comment block" "${d}" 1 \
  "matchers.rs:5: bare-parse:"

d="$(new_repo marker_in_string)"
cat >"${d}/crates/ravel-promql/src/matchers.rs" <<'RS'
fn fixture() -> Expr {
    promql_parser::parser::parse("up").expect("guarded-parse-allow: not a comment")
}
RS
check "the marker only counts from a comment, not a string" "${d}" 1 \
  "matchers.rs:2: bare-parse:"

# --- the first anchor: the funnel itself -----------------------------------

d="$(new_repo anchor_renamed)"
cat >"${d}/crates/ravel-promql/src/complexity_guard.rs" <<'RS'
pub fn parse_checked(query: &str) -> Result<Expr, GuardedParseError> {
    check(query)?;
    promql_parser::parser::parse(query).map_err(GuardedParseError::Parse)
}
RS
check "refuses to run when the funnel is gone" "${d}" 2 "expected exactly one"

d="$(new_repo anchor_duplicated)"
cat >>"${d}/crates/ravel-promql/src/complexity_guard.rs" <<'RS'

pub fn parse_guarded(query: &str) -> Result<Expr, GuardedParseError> {
    promql_parser::parser::parse(query).map_err(GuardedParseError::Parse)
}
RS
check "refuses when a second funnel appears" "${d}" 2 "expected exactly one"

d="$(new_repo anchor_no_parser)"
cat >"${d}/crates/ravel-promql/src/complexity_guard.rs" <<'RS'
pub fn parse_guarded(query: &str) -> Result<Expr, GuardedParseError> {
    check(query)?;
    somebody_elses_parse(query)
}
RS
check "refuses when the funnel no longer reaches a parser" "${d}" 2 \
  "no longer reaches a PromQL parser"

d="$(new_repo anchor_body_ends)"
cat >>"${d}/crates/ravel-promql/src/complexity_guard.rs" <<'RS'

fn later_in_the_same_file(query: &str) -> Expr {
    promql_parser::parser::parse(query).expect("parses")
}
RS
check "the exemption ends with the funnel body" "${d}" 1 \
  "complexity_guard.rs:12: bare-parse:"

d="$(new_repo anchor_scope)"
cat >"${d}/crates/ravel-promql/src/plan.rs" <<'RS'
pub fn parse_guarded(query: &str) -> Result<Expr, GuardedParseError> {
    promql_parser::parser::parse(query).map_err(GuardedParseError::Parse)
}
RS
check "a parse_guarded in another file is still a finding" "${d}" 1 \
  "plan.rs:2: bare-parse:"

# --- the second anchor: the cross-crate caller -----------------------------

d="$(new_repo second_anchor_gone)"
cat >"${d}/crates/ravel-query/src/engine.rs" <<'RS'
use promql_parser::parser::{Expr, Offset};

fn describe(expr: &Expr) -> String {
    format!("{expr}")
}
RS
check "refuses when the second root routes no parse through the funnel" "${d}" 2 \
  "routes nothing through"

# --- a scan that would find nothing ----------------------------------------

d="$(new_repo empty_roots)"
rm -f "${d}/crates/ravel-promql/src/complexity_guard.rs" \
  "${d}/crates/ravel-query/src/engine.rs"
check "refuses when the roots hold no Rust sources at all" "${d}" 2 \
  "no Rust sources"

d="$(new_repo missing_default_root)"
rm -rf "${d}/crates/ravel-query"
check "refuses when a default root is missing" "${d}" 2 "no such directory"

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
out="$(cd "${d}" && bash scripts/guards/check-guarded-promql-parse.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    %s\n' "an unknown option is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "an unknown option is a usage error" "${rc}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-guarded-promql-parse.sh \
  crates/ravel-promql/src crates/nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no such directory"* ]]; then
  printf 'ok    %s\n' "a named root that is not there is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "a named root that is not there is a usage error" "${rc}"
  fails=$((fails + 1))
fi

out="$(cd "${d}" && bash scripts/guards/check-guarded-promql-parse.sh \
  crates/ravel-promql/src 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"two roots"* ]]; then
  printf 'ok    %s\n' "scanning one root only is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "scanning one root only is a usage error" "${rc}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
