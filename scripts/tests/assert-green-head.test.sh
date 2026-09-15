#!/usr/bin/env bash
# Cases for scripts/guards/assert-green-head.sh: the merge gate that has to
# tell a real failure from a flake, and has to refuse to answer at all when
# the question cannot be asked about the current head.
#
# The verdicts differ mostly by exit code, so every case asserts the code.
# Stubbed `gh`; no network, no reruns actually issued.
#
# Run by hand:   bash scripts/tests/assert-green-head.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GUARD="${GUARD:-${SCRIPT_DIR}/guards/assert-green-head.sh}"

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

check_contains() {
  local label="$1" needle="$2" haystack="$3"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  want substring: %s\n  got: %s\n' "${label}" "${needle}" "${haystack}"
  fi
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

write_gh_stub() {
  cat >"$1/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
sub="${1:-}"; shift || true
case "${sub}" in
  repo) echo "myorg/myrepo" ;;
  pr)
    [[ "${1:-}" == "view" ]] || exit 1
    # The second read of the head SHA, used to catch a head that moved
    # mid-check, asks for exactly one field.
    if [[ "$*" == *"--jq"* ]]; then
      if [[ -f "${STUB_DIR}/head-second.txt" ]]; then
        cat "${STUB_DIR}/head-second.txt"
      else
        jq -r .headRefOid "${STUB_DIR}/pr.json"
      fi
      exit 0
    fi
    [[ -f "${STUB_DIR}/pr.json" ]] || exit 1
    cat "${STUB_DIR}/pr.json"
    ;;
  run)
    action="${1:-}"; shift || true
    case "${action}" in
      list)
        [[ -f "${STUB_DIR}/runs.txt" ]] || exit 1
        cat "${STUB_DIR}/runs.txt"
        ;;
      view)
        id="${1:-}"
        if [[ "$*" == *"--log-failed"* ]]; then
          # A fixture naming an error writes it to STDERR and fails, the way
          # gh reports a run that is still in progress.
          if [[ -f "${STUB_DIR}/log-err-${id}.txt" ]]; then
            cat "${STUB_DIR}/log-err-${id}.txt" >&2
            exit 1
          fi
          [[ -f "${STUB_DIR}/log-${id}.txt" ]] && cat "${STUB_DIR}/log-${id}.txt"
          exit 0
        fi
        [[ -f "${STUB_DIR}/jobs-${id}.txt" ]] || exit 1
        cat "${STUB_DIR}/jobs-${id}.txt"
        ;;
      rerun)
        printf '%s\n' "$*" >>"${STUB_DIR}/reruns.txt"
        ;;
      *) exit 1 ;;
    esac
    ;;
  *) exit 1 ;;
esac
STUB
  chmod +x "$1/gh"
}

new_case() {
  local dir="${work}/$1"
  mkdir -p "${dir}/bin" "${dir}/state"
  write_gh_stub "${dir}/bin"
  printf '%s\n' "${dir}"
}

run_in() {
  local dir="$1"; shift
  ( export STUB_DIR="${dir}" PATH="${dir}/bin:${PATH}" RAVEL_EPIC_STATE_DIR="${dir}/state"
    "$@" ) 2>&1
}

# A rollup entry in the CheckRun shape.
checkrun() {
  printf '{"name":"%s","status":"%s","conclusion":"%s"}' "$1" "$2" "$3"
}

pr_json() {
  local head="$1" rollup="$2"
  printf '{"state":"OPEN","mergeStateStatus":"CLEAN","headRefOid":"%s","headRefName":"feat/x","statusCheckRollup":[%s]}\n' \
    "${head}" "${rollup}"
}

sha="abc123def456abc123def456abc123def456abcd"

# --- green -------------------------------------------------------------
d="$(new_case green)"
pr_json "${sha}" "$(checkrun check COMPLETED SUCCESS),$(checkrun lint COMPLETED SUCCESS),$(checkrun k8s COMPLETED SKIPPED)" >"${d}/pr.json"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "all checks pass on the head: green (0)" "0" "${rc}"
check_contains "and counts the skipped one separately" "1 skipped" "${out}"

# An empty rollup is not a pass. A pull request whose checks have not been
# created yet has zero failing checks, and "zero failing" is the shape of a
# false green.
# Mutation: drop the total==0 branch.
d="$(new_case no_checks)"
pr_json "${sha}" "" >"${d}/pr.json"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "no checks at all is not green (5)" "5" "${rc}"
check_contains "and says an empty rollup is not a pass" "not a pass" "${out}"

# --- still running -----------------------------------------------------
d="$(new_case pending)"
pr_json "${sha}" "$(checkrun check IN_PROGRESS ''),$(checkrun lint COMPLETED SUCCESS)" >"${d}/pr.json"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "a running check means wait (2)" "2" "${rc}"

# --- the head moved under the query ------------------------------------
# Mutation: drop the second head read. The verdict then describes a commit
# that is no longer on the branch, which is the stale-run merge.
d="$(new_case moved)"
pr_json "${sha}" "$(checkrun check COMPLETED SUCCESS)" >"${d}/pr.json"
printf 'ffffffffffffffffffffffffffffffffffffffff\n' >"${d}/head-second.txt"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "a head that moved mid-check gives no verdict (5)" "5" "${rc}"
check_contains "and says the head moved" "head moved" "${out}"

# --- a red check, first time -------------------------------------------
d="$(new_case red_once)"
pr_json "${sha}" "$(checkrun check COMPLETED FAILURE)" >"${d}/pr.json"
printf '4242\tci\n' >"${d}/runs.txt"
printf 'check\tRun tests\n' >"${d}/jobs-4242.txt"
printf 'test ravel_sql::tests::parses ... FAILED\n' >"${d}/log-4242.txt"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "one red run asks for a rerun (3)" "3" "${rc}"
check_contains "and reports a signature" "signature " "${out}"
check_eq "and nothing was rerun without --rerun" "no" \
  "$([[ -f "${d}/reruns.txt" ]] && echo yes || echo no)"
check_eq "the attempt is recorded against the head sha" "1" \
  "$(jq -r --arg s "${sha}" '.ci["10"][$s].attempts' "${d}/state/ci-pr-10.json")"

# The same failure again on the same commit is a real failure, not a flake.
# Mutation: compare signatures only by job name, or skip the comparison.
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "the same signature twice escalates (1)" "1" "${rc}"
check_contains "and says it is real" "real failure" "${out}"

# A log with nothing recognisable in it still produces a signature, from the
# job and step names, and that signature cannot separate two failures in the
# same step. Say so rather than let the next run's "same signature twice"
# escalation rest on it silently.
# Mutation: drop the coarse branch.
d="$(new_case coarse)"
pr_json "${sha}" "$(checkrun check COMPLETED FAILURE)" >"${d}/pr.json"
printf '4242\tci\n' >"${d}/runs.txt"
printf 'check\tRun tests\n' >"${d}/jobs-4242.txt"
printf 'nothing recognisable here, just noise\n' >"${d}/log-4242.txt"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "an unparseable log still asks for a rerun (3)" "3" "${rc}"
check_contains "and says the signature is job-level only" "COARSE" "${out}"

# An empty log because the READER could not read it is a different fact from
# an empty log because the failure had nothing parseable in it, and the next
# action differs: re-read versus accept. gh announces the difference on
# stderr and a `2>/dev/null` merges the two into one wrong claim.
# Mutation: send the log fetch's stderr to /dev/null and report COARSE.
d="$(new_case unread)"
pr_json "${sha}" "$(checkrun check COMPLETED FAILURE)" >"${d}/pr.json"
printf '4242\tci\n' >"${d}/runs.txt"
printf 'check\tRun tests\n' >"${d}/jobs-4242.txt"
printf 'run 4242 is still in progress; logs will be available when it is complete\n' \
  >"${d}/log-err-4242.txt"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "an unreadable log still asks for a rerun (3)" "3" "${rc}"
check_contains "and says the reader failed, not the failure" "UNREAD" "${out}"
check_contains "and quotes gh's own reason" "still in progress" "${out}"
check_eq "and does not claim the log carried no identifier" "" \
  "$(printf '%s' "${out}" | grep -o COARSE || true)"

# ANSI colouring around a failure line must not hide it: this repo sets
# CARGO_TERM_COLOR: always, and the escape sits outside the matched text.
d="$(new_case ansi)"
pr_json "${sha}" "$(checkrun check COMPLETED FAILURE)" >"${d}/pr.json"
printf '4242\tci\n' >"${d}/runs.txt"
printf 'check\tRun tests\n' >"${d}/jobs-4242.txt"
# Shaped like a real runner line: a timestamp, then cargo's colour codes
# wrapping the token itself, which is what `CARGO_TERM_COLOR: always` emits.
printf '2026-09-15T10:00:00.0000000Z \033[0;31mtest ravel_sql::tests::alpha ... FAILED\033[0m\n' >"${d}/log-4242.txt"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "a colour-wrapped failure still parses (3)" "3" "${rc}"
check_contains "and the test name is in the signature" "ravel_sql::tests::alpha" "${out}"
check_eq "so it is not reported coarse" "" \
  "$(printf '%s' "${out}" | grep -o COARSE || true)"

# --- two different failures = a flake, and the budget is spent ----------
d="$(new_case flake)"
pr_json "${sha}" "$(checkrun check COMPLETED FAILURE)" >"${d}/pr.json"
printf '4242\tci\n' >"${d}/runs.txt"
printf 'check\tRun tests\n' >"${d}/jobs-4242.txt"
printf 'test ravel_sql::tests::alpha ... FAILED\n' >"${d}/log-4242.txt"
run_in "${d}" "${GUARD}" 10 >/dev/null; rc=$?
check_eq "first failure asks for a rerun (3)" "3" "${rc}"
printf 'test ravel_sql::tests::beta ... FAILED\n' >"${d}/log-4242.txt"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "a different failure the second time is a flake (4)" "4" "${rc}"
check_contains "and says the budget is spent" "budget" "${out}"

# --- a cancelled check belongs to the sweep ----------------------------
# Mutation: rerun it here. A job that hit its own timeout-minutes reports as
# cancelled, and a rerun on a faster runner hides the missing budget (#1590).
d="$(new_case cancelled)"
pr_json "${sha}" "$(checkrun features COMPLETED CANCELLED)" >"${d}/pr.json"
out="$(run_in "${d}" "${GUARD}" 10 --rerun)"; rc=$?
check_eq "a cancelled check is not rerun here (6)" "6" "${rc}"
check_contains "and points at the sweep" "ci-sweep-cancelled.sh" "${out}"
check_eq "and nothing was rerun" "no" \
  "$([[ -f "${d}/reruns.txt" ]] && echo yes || echo no)"

# --- --rerun issues exactly one rerun ----------------------------------
d="$(new_case rerun)"
pr_json "${sha}" "$(checkrun check COMPLETED FAILURE)" >"${d}/pr.json"
printf '4242\tci\n' >"${d}/runs.txt"
printf 'check\tRun tests\n' >"${d}/jobs-4242.txt"
printf 'thread tests::x panicked\n' >"${d}/log-4242.txt"
out="$(run_in "${d}" "${GUARD}" 10 --rerun)"; rc=$?
check_eq "--rerun still exits 3 (re-check later)" "3" "${rc}"
check_contains "and the rerun was issued for the failed jobs" "--failed" "$(cat "${d}/reruns.txt" 2>/dev/null)"

# --- could not ask -----------------------------------------------------
# Mutation: `|| true` on the pr view. An outage then reads as no failing
# checks, which is a green.
d="$(new_case gh_down)"
rm -f "${d}/pr.json"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "an unreadable pull request gives no verdict (5)" "5" "${rc}"
check_contains "and says it is not a green" "not a green" "${out}"

# --- a closed pull request ---------------------------------------------
d="$(new_case closed)"
printf '{"state":"MERGED","mergeStateStatus":"CLEAN","headRefOid":"%s","headRefName":"x","statusCheckRollup":[]}\n' "${sha}" >"${d}/pr.json"
out="$(run_in "${d}" "${GUARD}" 10)"; rc=$?
check_eq "a pull request that is not open gives no verdict (5)" "5" "${rc}"

# --- usage -------------------------------------------------------------
d="$(new_case usage)"
out="$(run_in "${d}" "${GUARD}")"; rc=$?
check_eq "no PR number is a usage error (64)" "64" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
