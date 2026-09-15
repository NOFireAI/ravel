#!/usr/bin/env bash
# Cases for check-promql-unreachable.sh. Add a case here before changing the
# rule, the way check-guarded-sql-parse.test.sh works for its sibling guard.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout.
#
# Run: bash scripts/guards/check-promql-unreachable.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-promql-unreachable.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-promql-unreachable-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed and an empty
# crates/ravel-promql/src ready for a fixture file.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/crates/ravel-promql/src"
  cp "${GUARD}" "${dir}/scripts/guards/check-promql-unreachable.sh"
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-promql-unreachable.sh 2>&1)" || rc=$?
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

# --- a bare unreachable! fails ----------------------------------------------

d="$(new_repo bare)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
fn reduce(op: TokenId) -> f64 {
    match op {
        T_SUM => 1.0,
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "flags a bare unreachable! with no marker" "${d}" 1 "aggregate.rs:4: bare-unreachable:"

# --- a marked one, reason on the same line, passes --------------------------

d="$(new_repo marked_inline)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
fn reduce(op: TokenId) -> f64 {
    match op {
        T_SUM => 1.0,
        // unreachable-allow: eval_aggregate's dispatch -- op is already
        // narrowed to T_SUM before this helper runs.
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "passes when the marker line itself carries a reason after --" "${d}" 0 "every unreachable! is documented"

# --- a marked one, reason wraps onto a following comment line, passes ------

d="$(new_repo marked_wrapped)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
fn reduce(op: TokenId) -> f64 {
    match op {
        T_SUM => 1.0,
        // unreachable-allow: eval_aggregate's dispatch --
        // op is already narrowed to T_SUM before this helper runs.
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "passes when the reason continues on the next comment line" "${d}" 0 "every unreachable! is documented"

# --- an empty marker reason fails --------------------------------------------

d="$(new_repo marker_no_reason)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
fn reduce(op: TokenId) -> f64 {
    match op {
        T_SUM => 1.0,
        // unreachable-allow: eval_aggregate's dispatch --
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "an empty reason after -- with no continuation does not suppress" "${d}" 1 "aggregate.rs:5: bare-unreachable:"

# --- a marker with an arm name but no -- at all fails -----------------------

d="$(new_repo marker_no_dashes)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
fn reduce(op: TokenId) -> f64 {
    match op {
        T_SUM => 1.0,
        // unreachable-allow: eval_aggregate's dispatch
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "a marker with no -- and no continuation does not suppress" "${d}" 1 "aggregate.rs:5: bare-unreachable:"

# --- the marker does not carry past its own comment block ------------------

d="$(new_repo marker_stale)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
// unreachable-allow: some other arm -- this reason belongs up here.
fn documented() {}

fn later(op: TokenId) -> f64 {
    match op {
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "a marker does not carry past its own comment block" "${d}" 1 "aggregate.rs:6: bare-unreachable:"

# --- prose and strings are not code -----------------------------------------

d="$(new_repo prose)"
cat >"${d}/crates/ravel-promql/src/aggregate.rs" <<'RS'
//! This module used to call unreachable!() before issue #1701 converted it.
/* A block comment mentioning unreachable!() too, spanning
   two lines. */
fn describe() -> &'static str {
    "unreachable!() in a string literal"
}

fn reduce(op: TokenId) -> f64 {
    match op {
        T_SUM => 1.0,
        // unreachable-allow: eval_aggregate's dispatch -- narrowed above.
        _ => unreachable!("no such aggregator"),
    }
}
RS
check "ignores unreachable! mentioned in comments and string literals" "${d}" 0 "every unreachable! is documented"

# --- zero occurrences is a failure, not a silent pass -----------------------

d="$(new_repo empty)"
cat >"${d}/crates/ravel-promql/src/lib.rs" <<'RS'
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
RS
check "exits 2 when the scan finds zero unreachable! occurrences" "${d}" 2 "found zero unreachable!"

# --- a missing source directory exits 2, not 0 ------------------------------

d="$(new_repo missing_dir)"
rm -rf "${d}/crates/ravel-promql/src"
check "exits 2 when the source directory is missing" "${d}" 2 "no such directory"

# --- usage -------------------------------------------------------------------

d="$(new_repo usage)"
out="$(cd "${d}" && bash scripts/guards/check-promql-unreachable.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    %s\n' "an unknown option is a usage error"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s\n' "an unknown option is a usage error" "${rc}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
