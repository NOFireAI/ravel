#!/usr/bin/env bash
# Cases for scripts/guards/assert-no-duplicate-dispatch.sh, the pre-dispatch
# duplicate gate. Stubbed `gh`, no network.
#
# The distinctions that cost something if they break, each with the mutation
# that breaks it:
#   - a failed query must not read as "nothing found"  (drop the ask() wrapper)
#   - #12 must not match #123                          (drop the boundary)
#   - a wide pull request's 150th file still counts    (use `pr list --json files`)
#   - a closed pull request outside the window is not  (drop the cutoff)
#     a reason to skip
#
# Run by hand:   bash scripts/tests/assert-no-duplicate-dispatch.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GUARD="${GUARD:-${SCRIPT_DIR}/guards/assert-no-duplicate-dispatch.sh}"

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
  repo)
    echo x >>"${STUB_DIR}/calls-repo"
    if [[ -f "${STUB_DIR}/fails-repo" ]] && (( $(cat "${STUB_DIR}/fails-repo") > 0 )); then
      echo $(( $(cat "${STUB_DIR}/fails-repo") - 1 )) >"${STUB_DIR}/fails-repo"
      echo "HTTP 502: transient" >&2
      exit 1
    fi
    echo "myorg/myrepo"
    ;;
  issue)
    [[ -f "${STUB_DIR}/issue.json" ]] || exit 1
    cat "${STUB_DIR}/issue.json"
    ;;
  pr)
    [[ "${1:-}" == "list" ]] || exit 1
    # prs-open.json answers --state open and prs-closed.json answers the
    # closed-since search; a case that sets neither serves prs.json to both.
    which=""
    case " $* " in
      *" --state open "*) which="open" ;;
      *" --search "*)
        which="closed"
        echo x >>"${STUB_DIR}/calls-closed"
        # The closed half must ask for one exact day over every state.
        [[ " $* " == *" --state all "* && " $* " =~ \ closed:[0-9]{4}-[0-9]{2}-[0-9]{2}\  ]] || {
          echo "stub: unexpected closed-list query: $*" >&2
          exit 3
        }
        ;;
    esac
    if [[ -n "${which}" && -f "${STUB_DIR}/prs-${which}.json" ]]; then
      cat "${STUB_DIR}/prs-${which}.json"
      exit 0
    fi
    [[ -f "${STUB_DIR}/prs.json" ]] || exit 1
    cat "${STUB_DIR}/prs.json"
    ;;
  api)
    # repos/<o>/<r>/pulls/<n>/files, already --jq'd to filenames by the caller.
    path="${1:-}"
    num="${path##*/pulls/}"; num="${num%%/*}"
    echo x >>"${STUB_DIR}/calls-${num}"
    # fails-N holds how many more calls for pull request N fail first.
    if [[ -f "${STUB_DIR}/gone-${num}" ]]; then
      echo "HTTP 404: Not Found" >&2
      exit 1
    fi
    if [[ -f "${STUB_DIR}/fails-${num}" ]]; then
      left="$(cat "${STUB_DIR}/fails-${num}")"
      if ((left > 0)); then
        echo $((left - 1)) >"${STUB_DIR}/fails-${num}"
        echo "HTTP 502: transient" >&2
        exit 1
      fi
    fi
    [[ -f "${STUB_DIR}/files-${num}.txt" ]] || exit 1
    cat "${STUB_DIR}/files-${num}.txt"
    ;;
  *) exit 1 ;;
esac
STUB
  chmod +x "$1/gh"
}

new_case() {
  local dir="${work}/$1"
  mkdir -p "${dir}/bin"
  write_gh_stub "${dir}/bin"
  printf '{"number":42,"state":"OPEN","title":"do the thing","stateReason":null}\n' >"${dir}/issue.json"
  printf '%s\n' "${dir}"
}

run_in() {
  local dir="$1"; shift
  ( export STUB_DIR="${dir}" PATH="${dir}/bin:${PATH}" ASK_BACKOFF_S=0; "$@" ) 2>&1
}

iso_days_ago() {
  local days="$1"
  if date -u -d "@$(( $(date +%s) - days * 86400 ))" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null; then
    return
  fi
  date -u -r "$(( $(date +%s) - days * 86400 ))" +%Y-%m-%dT%H:%M:%SZ
}

# --- nothing overlapping ----------------------------------------------
d="$(new_case clean)"
printf '[{"number":7,"title":"unrelated","state":"OPEN","body":"nothing here","headRefName":"x","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/other.rs\n' >"${d}/files-7.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "no reference and no overlap: dispatch (0)" "0" "${rc}"
check_contains "and says so" "OK" "${out}"

# --- an open pull request already addresses the issue ------------------
d="$(new_case addressed_open)"
printf '[{"number":8,"title":"fix the thing","state":"OPEN","body":"Fixes: #42","headRefName":"y","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "an open PR that names the issue: skip (65)" "65" "${rc}"
check_contains "and names it" "#8" "${out}"

# --- a merged pull request inside the window ---------------------------
d="$(new_case addressed_merged)"
merged="$(iso_days_ago 3)"
printf '[{"number":9,"title":"done already","state":"MERGED","body":"Fixes: #42","headRefName":"z","updatedAt":"%s","mergedAt":"%s","closedAt":"%s"}]\n' \
  "${merged}" "${merged}" "${merged}" >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "a recently merged PR that closes the issue: skip (65)" "65" "${rc}"

# --- cited is not closed -----------------------------------------------
#
# This repository's convention makes the difference load-bearing: `Fixes: #N`
# resolves the issue, `Refs: #N` says related and explicitly does not. Treating
# a citation as "already addressed" refuses the first genuine dispatch and
# pushes the operator to DISPATCH_SKIP_DUPLICATE_CHECK=1, which trains the
# reflex that flag exists to avoid.
#
# Measured against live data before the fix: issue #1790 is cited with `Refs:`
# by two pull requests and closed by neither, and the guard refused it.
# Mutation: match a bare `#<n>` again and this case fails.
d="$(new_case refs_only)"
printf '[{"number":20,"title":"unrelated work","state":"OPEN","body":"Refs: #42","headRefName":"r","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "a PR citing the issue with Refs: does not block (0)" "0" "${rc}"
check_contains "but is reported" "do not close it" "${out}"
check_contains "and names the PR" "#20" "${out}"

# A bare mention in prose is the same: information, not a refusal.
d="$(new_case bare_mention)"
printf '[{"number":21,"title":"see also","state":"OPEN","body":"related to #42 but separate","headRefName":"m","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "a bare mention does not block (0)" "0" "${rc}"

# Every GitHub closing keyword refuses, with and without the colon.
for kw in "Fixes: #42" "Fixed #42" "Closes #42" "Closed: #42" "Resolves: #42" "resolve #42" "fix #42"; do
  d="$(new_case "kw_$(printf '%s' "${kw}" | tr -cd '[:alnum:]')")"
  printf '[{"number":22,"title":"work","state":"OPEN","body":"%s","headRefName":"k","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' "${kw}" >"${d}/prs.json"
  out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
  check_eq "'${kw}' refuses (65)" "65" "${rc}"
done

# The boundary still holds on the closing-keyword path. TRAILING side.
d="$(new_case kw_boundary)"
printf '[{"number":23,"title":"other","state":"OPEN","body":"Fixes: #421","headRefName":"b","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "Fixes: #421 does not close #42 (0)" "0" "${rc}"

# LEADING side. The case above exercises only the trailing boundary, and the
# first version of the keyword test had none on the left: any word ENDING in a
# keyword refused, which is the same false-positive class the closing-keyword
# change was written to remove. GitHub closes nothing on `hotfix #42`.
# Mutation: drop the `(^|[^0-9A-Za-z])` prefix from closing_kw; each of these
# then refuses.
for word in "hotfix" "bugfix" "suffix:" "prefix" "postfix" "affixes" "disclosed" "unresolved"; do
  d="$(new_case "lead_$(printf '%s' "${word}" | tr -cd '[:alnum:]')")"
  printf '[{"number":24,"title":"work","state":"OPEN","body":"%s #42","headRefName":"l","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' "${word}" >"${d}/prs.json"
  out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
  check_eq "'${word} #42' is not a closing keyword (0)" "0" "${rc}"
done

# Case is not part of the keyword: GitHub closes on `FIXES #42` the same as
# `Fixes: #42`. Matching only a capitalised first letter let an all-caps body
# read as clean, so a resolved ticket allowed a second dispatch -- the
# false-clean direction this guard's header calls the only costly answer, and
# the opposite of the boundary bugs above.
# Mutation: drop the `; "i"` flag from the addressing predicate.
for kw in "FIXES #42" "CLOSES #42" "RESOLVES #42" "fixes #42" "Fixed #42"; do
  d="$(new_case "case_$(printf '%s' "${kw}" | tr -cd '[:alnum:]')")"
  printf '[{"number":26,"title":"work","state":"OPEN","body":"%s","headRefName":"c","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' "${kw}" >"${d}/prs.json"
  out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
  check_eq "'${kw}' refuses whatever its case (65)" "65" "${rc}"
done

# A colon absorbs the space. GitHub closes on `Fixes:#42`, and requiring
# whitespace unconditionally missed it.
# Mutation: put `[:]?[[:space:]]+` back as the separator.
d="$(new_case colon_nospace)"
printf '[{"number":27,"title":"work","state":"OPEN","body":"Fixes:#42","headRefName":"c","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "'Fixes:#42' refuses (65)" "65" "${rc}"

# Something must still separate the keyword from the ref.
d="$(new_case no_sep_digits)"
printf '[{"number":28,"title":"work","state":"OPEN","body":"Fixes42 #42","headRefName":"c","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "'Fixes42 #42' is not a closing keyword (0)" "0" "${rc}"

# GitHub's set has no -ing forms, so neither does this. `fixing #42` closes
# nothing on merge and must not refuse a dispatch.
# Mutation: put `ing` back in the alternation.
for word in "fixing" "closing" "resolving"; do
  d="$(new_case "ing_${word}")"
  printf '[{"number":25,"title":"work","state":"OPEN","body":"%s #42","headRefName":"g","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' "${word}" >"${d}/prs.json"
  out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
  check_eq "'${word} #42' does not close it (0)" "0" "${rc}"
done

# An old closed pull request is history, not a reason to skip work now.
# Mutation: drop the cutoff and every stale PR blocks forever.
d="$(new_case addressed_old)"
old="$(iso_days_ago 60)"
printf '[{"number":10,"title":"ancient","state":"CLOSED","body":"Refs: #42","headRefName":"z","updatedAt":"%s","mergedAt":null,"closedAt":"%s"}]\n' \
  "${old}" "${old}" >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "a PR closed outside the window does not block (0)" "0" "${rc}"

# --- #12 is not #123 ---------------------------------------------------
# Mutation: match on a bare "#" + number with no boundary.
d="$(new_case boundary)"
printf '[{"number":11,"title":"other work","state":"OPEN","body":"Fixes: #421","headRefName":"b","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/unrelated.rs\n' >"${d}/files-11.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "#421 does not count as a reference to #42" "0" "${rc}"

# --- file collision with an OPEN pull request --------------------------
d="$(new_case collide)"
printf '[{"number":12,"title":"in flight","state":"OPEN","body":"no issue reference","headRefName":"c","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/mine.rs\nsrc/other.rs\n' >"${d}/files-12.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs,src/new.rs)"; rc=$?
check_eq "an open PR on a predicted file collides (66)" "66" "${rc}"
check_contains "and names the shared file" "src/mine.rs" "${out}"

# The 150th file of a wide pull request counts the same as the first.
# Mutation: read files from `gh pr list --json files`, which caps at 100.
d="$(new_case wide)"
printf '[{"number":13,"title":"wide change","state":"OPEN","body":"","headRefName":"d","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
for i in $(seq 1 149); do printf 'src/pad_%s.rs\n' "${i}"; done >"${d}/files-13.txt"
printf 'src/mine.rs\n' >>"${d}/files-13.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "overlap past the 100-file cap is still found (66)" "66" "${rc}"

# --- closed-in-window overlap is informational -------------------------
d="$(new_case closed_overlap)"
recent="$(iso_days_ago 2)"
printf '[{"number":14,"title":"landed last week","state":"MERGED","body":"","headRefName":"e","updatedAt":"%s","mergedAt":"%s","closedAt":"%s"}]\n' \
  "${recent}" "${recent}" "${recent}" >"${d}/prs.json"
printf 'src/mine.rs\n' >"${d}/files-14.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a merged PR touching the same file is a note, not a block (0)" "0" "${rc}"
check_contains "and says to read it first" "NOTE" "${out}"

# --- the issue is already closed --------------------------------------
d="$(new_case closed_issue)"
printf '{"number":42,"state":"CLOSED","title":"already done","stateReason":"COMPLETED"}\n' >"${d}/issue.json"
printf '[]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "a closed issue is not dispatched (65)" "65" "${rc}"

# --- could not ask is not a clean answer -------------------------------
# Mutation: `|| true` on any of the gh calls. An outage then reads as
# "nothing overlapping" and every queued task dispatches on top of live work.
d="$(new_case gh_down_prs)"
rm -f "${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "an unreadable PR list exits 69, not 0" "69" "${rc}"
check_contains "and refuses to call it clean" "refusing to report a clean answer" "${out}"

d="$(new_case gh_down_issue)"
rm -f "${d}/issue.json"
printf '[]\n' >"${d}/prs.json"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "an unreadable issue exits 69, not 0" "69" "${rc}"

d="$(new_case gh_down_files)"
printf '[{"number":15,"title":"in flight","state":"OPEN","body":"","headRefName":"f","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
rm -f "${d}/files-15.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "an unreadable file list exits 69, not 0" "69" "${rc}"
check_eq "after exactly ASK_ATTEMPTS tries" "3" "$(wc -l <"${d}/calls-15" | tr -d ' ')"

# --- a transient failure is retried, not reported -----------------------
# Mutation: no retry loop in ask(). One failed call among a few hundred then
# refuses every dispatch, and the refusal is the only signal anyone sees.
d="$(new_case gh_blip_files)"
printf '[{"number":16,"title":"in flight","state":"OPEN","body":"","headRefName":"g","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/other.rs\n' >"${d}/files-16.txt"
echo 2 >"${d}/fails-16"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "two transient failures then success: dispatch (0)" "0" "${rc}"
check_eq "on the third try" "3" "$(wc -l <"${d}/calls-16" | tr -d ' ')"

d="$(new_case gh_blip_overlap)"
printf '[{"number":17,"title":"in flight","state":"OPEN","body":"","headRefName":"h","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/mine.rs\n' >"${d}/files-17.txt"
echo 1 >"${d}/fails-17"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a retried file list still finds the overlap (66)" "66" "${rc}"

d="$(new_case gh_down_reason)"
printf '[{"number":18,"title":"in flight","state":"OPEN","body":"","headRefName":"i","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/other.rs\n' >"${d}/files-18.txt"
echo 9 >"${d}/fails-18"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "failures past ASK_ATTEMPTS exit 69" "69" "${rc}"
check_contains "and show gh's own error" "HTTP 502" "${out}"

# --- a full pull request list may be truncated --------------------------
# Mutation: drop the length check. A busy fortnight with more pull requests
# than the limit then never lists the oldest ones in the window, and an
# overlap with one of them reads as clean.
d="$(new_case truncated_list)"
printf '[{"number":19,"title":"a","state":"OPEN","body":"","headRefName":"j","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null},{"number":20,"title":"b","state":"OPEN","body":"","headRefName":"k","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/other.rs\n' >"${d}/files-19.txt"
printf 'src/other.rs\n' >"${d}/files-20.txt"
out="$(PR_LIST_LIMIT=2 run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a list as long as its limit exits 69" "69" "${rc}"
check_contains "and says it may be truncated" "may be truncated" "${out}"
out="$(PR_LIST_LIMIT=3 run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a list shorter than its limit is complete (0)" "0" "${rc}"

# --- both halves of the candidate list are asked -------------------------
# Mutation: drop either list_prs call. An old open pull request then goes
# unchecked, or a recent merge that already did the work does.
d="$(new_case open_half)"
printf '[{"number":21,"title":"old but open","state":"OPEN","body":"","headRefName":"l","updatedAt":"2026-01-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs-open.json"
printf '[]\n' >"${d}/prs-closed.json"
printf 'src/mine.rs\n' >"${d}/files-21.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "an open pull request only the open list returns blocks (66)" "66" "${rc}"

d="$(new_case closed_half)"
recent="$(iso_days_ago 1)"
printf '[]\n' >"${d}/prs-open.json"
printf '[{"number":22,"title":"done yesterday","state":"MERGED","body":"Fixes: #42","headRefName":"m","updatedAt":"%s","mergedAt":"%s","closedAt":"%s"}]\n' \
  "${recent}" "${recent}" "${recent}" >"${d}/prs-closed.json"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a merge only the closed-since list returns addresses the issue (65)" "65" "${rc}"
# Mutation: one ">=" query for the whole window. The stub refuses that
# shape, and the window takes one query per day, the cutoff day included.
check_eq "one closed-list query per day of the 14-day window" "15" \
  "$(wc -l <"${d}/calls-closed" | tr -d ' ')"

# --- a missing resource is not retried ---------------------------------
d="$(new_case gone_files)"
printf '[{"number":23,"title":"deleted","state":"OPEN","body":"","headRefName":"n","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
touch "${d}/gone-23"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a 404 still refuses (69)" "69" "${rc}"
check_eq "after exactly one call" "1" "$(wc -l <"${d}/calls-23" | tr -d ' ')"

# --- resolving the repository is retried too ------------------------------
d="$(new_case repo_blip)"
printf '[]\n' >"${d}/prs.json"
echo 2 >"${d}/fails-repo"
out="$(run_in "${d}" "${GUARD}" --issue 42)"; rc=$?
check_eq "two failed repo lookups then success: dispatch (0)" "0" "${rc}"
check_eq "on the third try" "3" "$(wc -l <"${d}/calls-repo" | tr -d ' ')"

# --- usage -------------------------------------------------------------
d="$(new_case usage)"
out="$(run_in "${d}" "${GUARD}")"; rc=$?
check_eq "no --issue is a usage error (64)" "64" "${rc}"
out="$(run_in "${d}" "${GUARD}" --issue abc)"; rc=$?
check_eq "a non-numeric issue is a usage error (64)" "64" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
