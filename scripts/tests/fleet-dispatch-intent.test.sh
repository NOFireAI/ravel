#!/usr/bin/env bash
# Cases for scripts/fleet-dispatch-intent.sh, the one chokepoint every
# dispatch passes through.
#
# The script had no suite before the duplicate-work guard was wired into
# it, which is how its `|| true` on the comment read survived: an API
# failure produced an empty intent history and every dispatch looked clean.
# These cases pin both refusals and, more importantly, that neither one can
# be reached by accident when the answer is "could not ask".
#
# `gh` and both guards are stubbed; nothing hits the network.
#
# Run by hand:   bash scripts/tests/fleet-dispatch-intent.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INTENT="${INTENT:-${SCRIPT_DIR}/fleet-dispatch-intent.sh}"

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

# A case directory carries its own copy of the script plus stub guards, so
# the real guards are never invoked and no network call can happen.
new_case() {
  local name="$1" dup_exit="${2:-0}"
  local dir="${work}/${name}"
  mkdir -p "${dir}/bin" "${dir}/guards"
  cp "${INTENT}" "${dir}/fleet-dispatch-intent.sh"
  chmod +x "${dir}/fleet-dispatch-intent.sh"

  # The fresh-ref guard always passes here; its own behaviour is not what
  # these cases are about.
  printf '#!/usr/bin/env bash\nexit 0\n' >"${dir}/guards/assert-fresh-dispatch-ref.sh"
  # The duplicate-work guard records that it ran, with its arguments, and
  # exits with the code this case asked for.
  cat >"${dir}/guards/assert-no-duplicate-dispatch.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"${dir}/dup-calls.txt"
exit ${dup_exit}
STUB
  chmod +x "${dir}/guards/"*.sh

  cat >"${dir}/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
if [[ "${1:-}" == "issue" && "${2:-}" == "view" ]]; then
  [[ -f "${STUB_DIR}/comments.txt" ]] || exit 1
  cat "${STUB_DIR}/comments.txt"
  exit 0
fi
if [[ "${1:-}" == "issue" && "${2:-}" == "comment" ]]; then
  printf '%s\n' "$*" >>"${STUB_DIR}/posted.txt"
  exit 0
fi
exit 1
STUB
  chmod +x "${dir}/bin/gh"
  : >"${dir}/comments.txt"
  printf '%s\n' "${dir}"
}

run_in() {
  local dir="$1"; shift
  ( export STUB_DIR="${dir}" PATH="${dir}/bin:${PATH}"
    "${dir}/fleet-dispatch-intent.sh" "$@" ) 2>&1
}

# --- the clean path ----------------------------------------------------
d="$(new_case clean 0)"
out="$(run_in "${d}" intent 900 101 deadbeef)"; rc=$?
check_eq "a clean ticket dispatches (0)" "0" "${rc}"
check_eq "and the duplicate guard was actually consulted" "1" \
  "$(wc -l <"${d}/dup-calls.txt" | tr -d ' ')"
check_contains "with the issue number" "--issue 101" "$(cat "${d}/dup-calls.txt")"
check_contains "and an intent comment was posted" "dispatch-intent" "$(cat "${d}/posted.txt")"

# --- the guard refuses -------------------------------------------------
# 65: a pull request already addresses the issue.
# Mutation: drop the `if [[ ${dup_rc} -ne 0 ]]` block and the dispatch
# proceeds over work someone else is already doing.
d="$(new_case addressed 65)"
out="$(run_in "${d}" intent 900 101 deadbeef)"; rc=$?
check_eq "an already-addressed ticket refuses (65)" "65" "${rc}"
check_eq "and NO intent comment was posted" "no" \
  "$([[ -f "${d}/posted.txt" ]] && echo yes || echo no)"

# 66: an open pull request is on the predicted files.
d="$(new_case collide 66)"
out="$(run_in "${d}" intent 900 101 deadbeef)"; rc=$?
check_eq "a file collision refuses (66)" "66" "${rc}"
check_eq "and posts nothing" "no" \
  "$([[ -f "${d}/posted.txt" ]] && echo yes || echo no)"

# 69: could not ask. This must refuse for the same reason the comment read
# above refuses: an unreadable GitHub is exactly when a dispatch is most
# likely to be a retry of one that already started.
# Mutation: treat 69 as clean and the guard becomes decorative in an outage.
d="$(new_case unknown 69)"
out="$(run_in "${d}" intent 900 101 deadbeef)"; rc=$?
check_eq "could-not-ask refuses too (69)" "69" "${rc}"
check_contains "and says a failed question is not a clean answer" "not a clean answer" "${out}"

# --- predicted paths reach the guard -----------------------------------
d="$(new_case paths 0)"
out="$( export DISPATCH_PATHS="crates/a/src/lib.rs,crates/b/src/lib.rs"
        run_in "${d}" intent 900 101 deadbeef )"; rc=$?
check_eq "DISPATCH_PATHS is accepted (0)" "0" "${rc}"
check_contains "and forwarded to the guard" \
  "--paths crates/a/src/lib.rs,crates/b/src/lib.rs" "$(cat "${d}/dup-calls.txt")"

# --- the deliberate second dispatch ------------------------------------
d="$(new_case override 65)"
out="$( export DISPATCH_SKIP_DUPLICATE_CHECK=1
        run_in "${d}" intent 900 101 deadbeef )"; rc=$?
check_eq "the override proceeds past a refusing guard (0)" "0" "${rc}"
check_eq "and the guard was not run at all" "no" \
  "$([[ -f "${d}/dup-calls.txt" ]] && echo yes || echo no)"

# --- a non-numeric ticket ----------------------------------------------
# The guard looks up an issue; a ticket that is not one cannot be checked,
# and guessing at it would refuse real work. Skipped, not failed.
d="$(new_case nonnumeric 65)"
out="$(run_in "${d}" intent 900 'T4-rewrite' deadbeef)"; rc=$?
check_eq "a non-numeric ticket skips the lookup (0)" "0" "${rc}"
check_eq "and never called the guard" "no" \
  "$([[ -f "${d}/dup-calls.txt" ]] && echo yes || echo no)"

# A `#101` spelling is the same ticket as `101`.
d="$(new_case hashform 0)"
out="$(run_in "${d}" intent 900 '#101' deadbeef)"; rc=$?
check_eq "a #-prefixed ticket is looked up (0)" "0" "${rc}"
check_contains "with the # stripped" "--issue 101" "$(cat "${d}/dup-calls.txt")"

# --- the duplicate check runs AFTER the dangling-intent check ----------
# A dangling intent is the cheaper, local refusal and must still fire.
d="$(new_case dangling 0)"
printf 'dispatch-intent nonce=111-1 ticket=101 ref=abc\n' >"${d}/comments.txt"
out="$(run_in "${d}" intent 900 101 deadbeef)"; rc=$?
check_eq "a dangling intent still refuses first (65)" "65" "${rc}"
check_contains "and says so" "dangling intent" "${out}"

# --- an unreadable comment history still refuses -----------------------
d="$(new_case nohistory 0)"
rm -f "${d}/comments.txt"
out="$(run_in "${d}" intent 900 101 deadbeef)"; rc=$?
check_eq "an unreadable intent history refuses (69)" "69" "${rc}"
check_contains "and names UNKNOWN" "UNKNOWN" "${out}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
