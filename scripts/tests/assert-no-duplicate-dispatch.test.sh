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
    # repos/<o>/<r>/pulls/<n>/files. The caller's own --jq runs over the
    # fixture: files-N.json is the endpoint's array (filename and patch),
    # files-N.txt a bare list of filenames with no patch.
    path="${1:-}"
    jq_expr=""
    args=("$@")
    for ((i = 0; i < ${#args[@]}; i++)); do
      [[ "${args[i]}" == "--jq" ]] && jq_expr="${args[i + 1]:-}"
    done
    # repos/<o>/<r>/compare/<base>...<head>. mb-<head> holds the merge base;
    # without one the answer is origin/main of the repository the guard runs
    # in. compare-down-<head> fails every call.
    if [[ "${path}" == */compare/* ]]; then
      spec="${path##*/compare/}"; head="${spec##*...}"
      echo "${spec}" >>"${STUB_DIR}/calls-compare-${head}"
      if [[ -f "${STUB_DIR}/compare-down-${head}" ]]; then
        echo "HTTP 502: transient" >&2
        exit 1
      fi
      if [[ -f "${STUB_DIR}/mb-${head}" ]]; then
        mb="$(cat "${STUB_DIR}/mb-${head}")"
      else
        mb="$(git rev-parse --verify --quiet refs/remotes/origin/main)" || exit 1
      fi
      jq -r "${jq_expr:-.}" <<<"$(jq -n --arg s "${mb}" '{merge_base_commit: {sha: $s}}')"
      exit 0
    fi
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
    # patch-down-N fails every call for pull request N that asks for patches,
    # so a guard that fetched them separately and fell back to filenames
    # would read the failure as no overlap.
    if [[ -f "${STUB_DIR}/patch-down-${num}" && "${jq_expr}" == *patch* ]]; then
      echo "HTTP 502: transient" >&2
      exit 1
    fi
    if [[ -f "${STUB_DIR}/files-${num}.json" ]]; then
      src="$(cat "${STUB_DIR}/files-${num}.json")"
    elif [[ -f "${STUB_DIR}/files-${num}.txt" ]]; then
      src="$(jq -R . "${STUB_DIR}/files-${num}.txt" | jq -s 'map({filename: ., patch: null})')"
    else
      exit 1
    fi
    if [[ -n "${jq_expr}" ]]; then
      jq -r "${jq_expr}" <<<"${src}"
    else
      printf '%s\n' "${src}"
    fi
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

# --- a held draft is context, not a collision ---------------------------
# Mutation: drop `and .isDraft` from the state mapping, or test the state
# before the mapping. A held draft once blocked two epics' tickets for a day.
d="$(new_case draft_only)"
printf '[{"number":15,"title":"held draft","state":"OPEN","isDraft":true,"body":"","headRefName":"f","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/mine.rs\n' >"${d}/files-15.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "a draft PR on a predicted file is not a collision (0)" "0" "${rc}"
check_contains "and is listed as a draft" "[DRAFT]" "${out}"
check_contains "under the context heading" "NOTE" "${out}"

# An open non-draft beside a draft still refuses, and the draft is listed
# under the context heading rather than as the reason.
d="$(new_case draft_and_open)"
printf '[{"number":16,"title":"held draft","state":"OPEN","isDraft":true,"body":"","headRefName":"g","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null},{"number":17,"title":"in flight","state":"OPEN","isDraft":false,"body":"","headRefName":"h","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
printf 'src/mine.rs\n' >"${d}/files-16.txt"
printf 'src/mine.rs\n' >"${d}/files-17.txt"
out="$(run_in "${d}" "${GUARD}" --issue 42 --paths src/mine.rs)"; rc=$?
check_eq "an open non-draft beside a draft still collides (66)" "66" "${rc}"
check_contains "the open one is the reason" "#17 [OPEN]" "${out}"
check_contains "the draft is context" "not a collision" "${out}"

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

# --- hunk, symbol and docs-section entries -------------------------------
#
# One base tree for every case below, committed in a scratch repository with
# refs/remotes/origin/main pointing at it, which is the guard's default base.
# Line numbers in the comments are the base file's.
base="${work}/base"
mkdir -p "${base}/src" "${base}/docs" "${base}/scripts"
cat >"${base}/src/lib.rs" <<'EOF'
use std::fmt;

pub fn alpha(x: u32) -> u32 {
    let y = x + 1;
    y * 2
}

pub struct Beta {
    a: u32,
}

impl Beta {
    pub fn gamma(&self) -> u32 {
        self.a
    }
}

const LIMIT: [u8; 2] = [
    1, 2,
];
EOF
# lib.rs: alpha 3-6, Beta 8-10 and 12-16, gamma 13-15, LIMIT 18-20.
cat >"${base}/scripts/tool.sh" <<'EOF'
#!/usr/bin/env bash
helper() {
  echo "${1}"
}

function other {
  echo two
}
EOF
# tool.sh: helper 2-4, other 6-8.
cat >"${base}/docs/guide.md" <<'EOF'
# Guide

Intro text.

## Install

Run the installer.

### Options

Flags here.

## Usage

Use it.
```
## not a heading inside a fence
```
More usage.
EOF
# guide.md: Install 5-12 (Options 9-12 nested in it), Usage 13-19; line 17
# is inside a fence and is not a heading.
git -C "${base}" init -q
git -C "${base}" add -A
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -m base
git -C "${base}" update-ref refs/remotes/origin/main HEAD

# open_pr <case> <n>: one open, non-draft pull request #n into main, whose
# head commit the compare stub knows as h<n>.
open_pr() {
  printf '[{"number":%s,"title":"in flight","state":"OPEN","body":"","headRefName":"p","baseRefName":"main","headRefOid":"h%s","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' "$2" "$2" >"$1/prs.json"
}
# pr_patch <case> <n> <file> <patch line>...: pull request #n changes <file>
# with the given patch, as the files endpoint returns it.
pr_patch() {
  local dir="$1" num="$2" file="$3"
  shift 3
  jq -n --arg f "${file}" --arg p "$(printf '%s\n' "$@")" '[{filename: $f, patch: $p}]' \
    >"${dir}/files-${num}.json"
}
run_base() {
  local dir="$1"; shift
  (cd "${base}" && run_in "${dir}" "${GUARD}" "$@")
}

# Patch A changes lib.rs line 4, inside alpha.
patch_a=('@@ -2,5 +2,5 @@' ' ' ' pub fn alpha(x: u32) -> u32 {' '-    let y = x + 1;' '+    let y = x + 2;' '     y * 2' ' }')
# Patch B inserts 20 lines after line 1 and changes line 14, inside gamma.
# Every head line from 2 on is 20 past its base line.
patch_b=('@@ -1,2 +1,22 @@' ' use std::fmt;')
for i in $(seq 1 20); do patch_b+=("+// pad ${i}"); done
patch_b+=(' ' '@@ -12,4 +32,4 @@' ' impl Beta {' '     pub fn gamma(&self) -> u32 {' '-        self.a' '+        self.a + 1' '     }')
# Patch C changes lib.rs line 19, inside LIMIT.
patch_c=('@@ -18,3 +18,3 @@' ' const LIMIT: [u8; 2] = [' '-    1, 2,' '+    1, 3,' ' ];')

# A hunk overlapping the range refuses, the range end included.
# Wrong: a range entry read as a literal file name never matches (0); an
# exclusive end misses line 14 (0).
d="$(new_case hunk_overlap)"
open_pr "${d}" 30
pr_patch "${d}" 30 src/lib.rs "${patch_b[@]}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L12-L14)"; rc=$?
check_eq "a hunk inside the range collides (66)" "66" "${rc}"
check_contains "and names the entry" "src/lib.rs:L12-L14" "${out}"

# The same file, a range no hunk reaches, passes. Patch B's head lines 2-21
# are new, so reading the head side of the hunk header puts lines 3-6 inside.
# Wrong: the whole-file rule (66); head-side line numbers (66).
d="$(new_case hunk_elsewhere)"
open_pr "${d}" 31
pr_patch "${d}" 31 src/lib.rs "${patch_b[@]}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L3-L6)"; rc=$?
check_eq "the same file, a different hunk, passes (0)" "0" "${rc}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs)"; rc=$?
check_eq "while the bare path still collides on the whole file (66)" "66" "${rc}"

# A symbol whose body the patch changes refuses.
# Wrong: the anchor line alone as the symbol (0); the block cut at the first
# semicolon on the anchor line, whatever the bracket depth (0, for LIMIT).
d="$(new_case symbol_touched)"
open_pr "${d}" 32
pr_patch "${d}" 32 src/lib.rs "${patch_a[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#alpha')"; rc=$?
check_eq "a symbol whose body the PR changes collides (66)" "66" "${rc}"
check_contains "and names the entry" "src/lib.rs#alpha" "${out}"
d="$(new_case symbol_multiline_const)"
open_pr "${d}" 33
pr_patch "${d}" 33 src/lib.rs "${patch_c[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#LIMIT')"; rc=$?
check_eq "a const whose initializer spans lines collides on its body (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#Beta')"; rc=$?
check_eq "a type's struct and impl blocks do not reach LIMIT (0)" "0" "${rc}"

# A symbol the patch does not reach passes.
# Wrong: the whole-file rule (66); the block run to the end of the file
# instead of its closing brace (66, alpha would reach LIMIT).
d="$(new_case symbol_untouched)"
open_pr "${d}" 34
pr_patch "${d}" 34 src/lib.rs "${patch_c[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#alpha')"; rc=$?
check_eq "a symbol the PR does not touch passes (0)" "0" "${rc}"
d="$(new_case symbol_impl)"
open_pr "${d}" 35
pr_patch "${d}" 35 src/lib.rs "${patch_b[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#Beta')"; rc=$?
check_eq "an impl block's body counts for the type it names (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#alpha')"; rc=$?
check_eq "an insertion above a function is not inside it (0)" "0" "${rc}"

# Shell functions in both spellings.
patch_d=('@@ -6,3 +6,3 @@' ' function other {' '-  echo two' '+  echo three' ' }')
d="$(new_case shell_symbol)"
open_pr "${d}" 36
pr_patch "${d}" 36 scripts/tool.sh "${patch_d[@]}"
out="$(run_base "${d}" --issue 42 --paths 'scripts/tool.sh#other')"; rc=$?
check_eq "a shell function the PR changes collides (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'scripts/tool.sh#helper')"; rc=$?
check_eq "a shell function it does not change passes (0)" "0" "${rc}"

# A symbol that is not on the base is compared as the whole file, and says so.
d="$(new_case symbol_missing)"
open_pr "${d}" 37
pr_patch "${d}" 37 src/lib.rs "${patch_c[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#delta')"; rc=$?
check_eq "a symbol not on the base falls back to the whole file (66)" "66" "${rc}"
check_contains "and says it was not found" "not found" "${out}"

# Which base lines a hunk changes. Patch B inserts between lines 1 and 2,
# and lines 12, 13 and 15 are context in its second hunk; patch A replaces
# line 4. Wrong: only the line after an insertion (L1 passes), only the line
# before (L2 passes), the hunk header's whole range (L15 collides), the
# insertion rule applied to a replacement (L5 collides).
d="$(new_case changed_lines)"
open_pr "${d}" 38
pr_patch "${d}" 38 src/lib.rs "${patch_b[@]}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L1-L1)"; rc=$?
check_eq "an insertion counts against the line before it (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L2-L2)"; rc=$?
check_eq "and the line after it (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L15-L15)"; rc=$?
check_eq "a context line does not count (0)" "0" "${rc}"
pr_patch "${d}" 38 src/lib.rs "${patch_a[@]}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L5-L5)"; rc=$?
check_eq "a replacement does not reach the line after it (0)" "0" "${rc}"

# Every anchor form in the header, each with a body line below its anchor.
# Line 48 is inside after(), past every definition including broken(), whose
# braces never balance (the "{" in a string) and so ends before the next
# column-0 definition. Wrong: an anchor form missing from the pattern (its
# name falls back to the whole file, which still collides, so the positive
# case alone cannot see it; the line-48 case then collides); broken() run to
# the end of the file (line 48 collides).
cat >"${work}/kinds.rs" <<'EOF'
pub(crate) async fn one() {
    1;
}
pub enum Two {
    A,
}
pub trait Three {
    fn x(&self);
}
mod four {
    fn y() {}
}
static FIVE: [u8; 1] = [
    5,
];
type Six = Vec<
    u8,
>;
union Seven {
    a: u32,
}
extern "C" fn eight() {
    8;
}
const fn nine() {
    9;
}
/// Ten.
#[inline]
fn ten() {
    10;
}
pub unsafe fn eleven() {
    11;
}
default fn twelve() {
    12;
}
impl Clone for Thirteen {
    13;
}
fn broken() {
    let s = "{";
    s.len();
}

fn after() {
    48;
}
EOF
cp "${work}/kinds.rs" "${base}/src/kinds.rs"
git -C "${base}" add src/kinds.rs
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -m kinds
git -C "${base}" update-ref refs/remotes/origin/main HEAD
d="$(new_case symbol_kinds)"
open_pr "${d}" 39
for pair in one:2 Two:5 Three:8 four:11 FIVE:14 Six:17 Seven:20 eight:23 nine:26 ten:31 eleven:34 twelve:37 Thirteen:40 broken:44; do
  sym="${pair%%:*}"; line="${pair##*:}"
  # The removed line is the base's own, so the drift check passes and the
  # symbol's extent decides.
  pr_patch "${d}" 39 src/kinds.rs "@@ -${line},1 +${line},1 @@" "-$(sed -n "${line}p" "${work}/kinds.rs")" "+    new"
  out="$(run_base "${d}" --issue 42 --paths "src/kinds.rs#${sym}")"; rc=$?
  check_eq "#${sym}: a change on its line ${line} collides (66)" "66" "${rc}"
  pr_patch "${d}" 39 src/kinds.rs "@@ -48,1 +48,1 @@" "-    48;" "+    49;"
  out="$(run_base "${d}" --issue 42 --paths "src/kinds.rs#${sym}")"; rc=$?
  check_eq "#${sym}: a change on line 48 does not (0)" "0" "${rc}"
done
# Doc comments and attributes above the anchor are not part of it.
pr_patch "${d}" 39 src/kinds.rs "@@ -28,1 +28,1 @@" "-/// Ten." "+/// Ten, documented."
out="$(run_base "${d}" --issue 42 --paths 'src/kinds.rs#ten')"; rc=$?
check_eq "a doc comment above the anchor is not in the definition (0)" "0" "${rc}"

# docs/: the unit is the heading section of the hunk's base lines.
patch_e=('@@ -14,3 +14,3 @@' ' ' '-Use it.' '+Use it well.' ' ```')
patch_f=('@@ -18,2 +18,2 @@' ' ```' '-More usage.' '+More usage here.')
patch_g=('@@ -10,3 +10,3 @@' ' ' '-Flags here.' '+Flags listed here.' ' ')

# Same section refuses. Wrong: the heading line alone as the section (0);
# a fenced "## " line read as a heading, which ends Usage at line 16 (0).
d="$(new_case docs_same_section)"
open_pr "${d}" 40
pr_patch "${d}" 40 docs/guide.md "${patch_e[@]}"
out="$(run_base "${d}" --issue 42 --paths 'docs/guide.md#usage')"; rc=$?
check_eq "a docs edit in the same section collides (66)" "66" "${rc}"
d="$(new_case docs_fenced_heading)"
open_pr "${d}" 41
pr_patch "${d}" 41 docs/guide.md "${patch_f[@]}"
out="$(run_base "${d}" --issue 42 --paths 'docs/guide.md#usage')"; rc=$?
check_eq "a heading inside a code fence does not end the section (66)" "66" "${rc}"
# A docs range is compared by section, not by line. Wrong: line overlap (0).
out="$(run_base "${d}" --issue 42 --paths docs/guide.md:L14-L15)"; rc=$?
check_eq "a docs range collides with any edit in its sections (66)" "66" "${rc}"

# A nested subsection belongs to its parent. Wrong: the section ended at the
# next heading of any level (0).
d="$(new_case docs_subsection)"
open_pr "${d}" 42
pr_patch "${d}" 42 docs/guide.md "${patch_g[@]}"
out="$(run_base "${d}" --issue 42 --paths 'docs/guide.md#install')"; rc=$?
check_eq "an edit in a nested subsection collides with its parent (66)" "66" "${rc}"

# A different section passes. Wrong: the whole-file rule (66); the section
# run to the end of the file (66).
d="$(new_case docs_other_section)"
open_pr "${d}" 43
pr_patch "${d}" 43 docs/guide.md "${patch_e[@]}"
out="$(run_base "${d}" --issue 42 --paths 'docs/guide.md#install')"; rc=$?
check_eq "a docs edit in a different section passes (0)" "0" "${rc}"
out="$(run_base "${d}" --issue 42 --paths docs/guide.md:L5-L8)"; rc=$?
check_eq "a docs range in a different section passes (0)" "0" "${rc}"
out="$(run_base "${d}" --issue 42 --paths docs/guide.md)"; rc=$?
check_eq "a bare docs path keeps the whole-file rule (66)" "66" "${rc}"

# A fence closes only on the same character at least as long as its opener.
# nested.md: Example 3-11 holds a four-backtick fence (5-10) around a
# three-backtick one (6-9); Other 12-19 holds a tilde fence (14-18) around a
# three-backtick line. Lines 7 and 16 look like headings and are not.
cat >"${base}/docs/nested.md" <<'EOF'
# Nested

## Example

````markdown
```sh
## looks like a heading
echo edited
```
````

## Other

~~~
```
## also not a heading
Other text.
~~~

## Last

End.
EOF
# x.py: def main 3-6, a call main() at 9.
cat >"${base}/scripts/x.py" <<'EOF'
import sys

def main():
    a = 1
    b = 2
    return a + b

if __name__ == "__main__":
    main()
EOF
git -C "${base}" add docs/nested.md scripts/x.py
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -m nested
git -C "${base}" update-ref refs/remotes/origin/main HEAD

# Wrong: any fence line toggles (0, line 7 ends Example); a shorter fence of
# the same character closes (0, line 6 closes the four-backtick fence).
d="$(new_case docs_nested_fence)"
open_pr "${d}" 48
pr_patch "${d}" 48 docs/nested.md '@@ -7,3 +7,3 @@' ' ## looks like a heading' '-echo edited' '+echo edited twice' ' ```'
out="$(run_base "${d}" --issue 42 --paths 'docs/nested.md#example')"; rc=$?
check_eq "an edit inside a fence nested in a longer fence is in its section (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'docs/nested.md#other')"; rc=$?
check_eq "and not in the next section (0)" "0" "${rc}"
# Wrong: a fence of either character closes (0, line 15 closes the tilde
# fence and line 16 ends Other).
d="$(new_case docs_tilde_fence)"
open_pr "${d}" 49
pr_patch "${d}" 49 docs/nested.md '@@ -16,3 +16,3 @@' ' ## also not a heading' '-Other text.' '+Other text, edited.' ' ~~~'
out="$(run_base "${d}" --issue 42 --paths 'docs/nested.md#other')"; rc=$?
check_eq "a backtick line does not close a tilde fence (66)" "66" "${rc}"

# A range that starts past the base file's last line names nothing on the
# base, so it is compared as the whole file. Wrong: the start clamped to the
# last line (0, patch G is in Install and the last section is Usage); the
# range left as it is (0, it covers no section and no line).
d="$(new_case range_past_eof)"
open_pr "${d}" 50
pr_patch "${d}" 50 docs/guide.md "${patch_g[@]}"
out="$(run_base "${d}" --issue 42 --paths docs/guide.md:L40-L50)"; rc=$?
check_eq "a docs range past the last line falls back to the whole file (66)" "66" "${rc}"
check_contains "and says so" "past the last line" "${out}"
pr_patch "${d}" 50 src/lib.rs "${patch_a[@]}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L40-L50)"; rc=$?
check_eq "so does a code range past the last line (66)" "66" "${rc}"
check_contains "and says so" "past the last line" "${out}"
# A range on a file the base does not have (moved or deleted on main since the
# pull request forked) has nothing to number against. Wrong: the range read
# as given (0 when nothing else reads the base, the hunk is at line 10; with
# the drift check in place it still collides, and only the NOTE says why).
pr_patch "${d}" 50 src/gone.rs '@@ -10,1 +10,1 @@' '-old' '+new'
out="$(run_base "${d}" --issue 42 --paths src/gone.rs:L1-L2)"; rc=$?
check_eq "a range on a file not on the base falls back to the whole file (66)" "66" "${rc}"
check_contains "and says so" "src/gone.rs is not on origin/main" "${out}"
out="$(run_base "${d}" --issue 42 --paths 'src/gone.rs#alpha')"; rc=$?
check_eq "so does a symbol on a file not on the base (66)" "66" "${rc}"
check_contains "and says so" "'alpha' not found in src/gone.rs" "${out}"

# path#symbol on a file type with no anchor rules is compared as the whole
# file. Wrong: both rule sets on any other type (0, the shell form takes the
# call main() on line 9 for the definition); the Rust forms alone (66 via
# not found, with the wrong NOTE).
d="$(new_case symbol_other_type)"
open_pr "${d}" 51
pr_patch "${d}" 51 scripts/x.py '@@ -4,3 +4,3 @@' '     a = 1' '-    b = 2' '+    b = 3' '     return a + b'
out="$(run_base "${d}" --issue 42 --paths 'scripts/x.py#main')"; rc=$?
check_eq "a symbol entry on a .py file is compared as the whole file (66)" "66" "${rc}"
check_contains "and says the type is not supported" "symbol entries are not supported" "${out}"

# Base drift: the guard reads symbols and ranges from --base-ref, a pull
# request numbers its hunks on its own merge base. drifted has 20 lines
# prepended to lib.rs (alpha at 21-26); line4 has line 4 changed and line2
# has line 2 changed, each with every other line as on origin/main; tail has
# lines appended after the last one.
git -C "${base}" checkout -q -b drifted
{ for i in $(seq 1 20); do printf '// pad %s\n' "${i}"; done; git -C "${base}" show origin/main:src/lib.rs; } >"${work}/drifted.rs"
cp "${work}/drifted.rs" "${base}/src/lib.rs"
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -am drifted
git -C "${base}" update-ref refs/drift/main HEAD
git -C "${base}" show origin/main:src/lib.rs | sed '4s/x + 1/x + 5/' >"${base}/src/lib.rs"
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -am line4
git -C "${base}" update-ref refs/drift/line4 HEAD
git -C "${base}" show origin/main:src/lib.rs | sed '2s/^$/\/\/ two/' >"${base}/src/lib.rs"
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -am line2
git -C "${base}" update-ref refs/drift/line2 HEAD
{ git -C "${base}" show origin/main:src/lib.rs; printf '\nfn tail() {}\n'; } >"${base}/src/lib.rs"
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -am tail
git -C "${base}" update-ref refs/drift/tail HEAD
git -C "${base}" checkout -q -

# The merge base is origin/main unless a case says otherwise. Wrong: no
# merge-base check (0 for both, alpha read at 21-26 against a hunk at base
# line 4); every non-bare entry compared as the whole file (the exact cases
# below then collide).
d="$(new_case base_drift)"
open_pr "${d}" 52
pr_patch "${d}" 52 src/lib.rs "${patch_a[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#alpha' --base-ref refs/drift/main)"; rc=$?
check_eq "a symbol on a drifted base falls back to the whole file (66)" "66" "${rc}"
check_contains "and names the file and the drift" "NOTE  #52 src/lib.rs: changed on refs/drift/main since the pull request forked" "${out}"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L21-L26 --base-ref refs/drift/main)"; rc=$?
check_eq "a range on a drifted base falls back to the whole file (66)" "66" "${rc}"
check_contains "and says so" "since the pull request forked" "${out}"
# A file that differs only past the hunks still differs. Wrong: hunk lines
# compared against the base ref instead of the file compared (0).
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#gamma' --base-ref refs/drift/tail)"; rc=$?
check_eq "a change past the hunks is still drift (66)" "66" "${rc}"
check_contains "and says so" "changed on refs/drift/tail since the pull request forked" "${out}"
# The same file on the merge base and the base ref: exact.
git -C "${base}" rev-parse refs/drift/tail >"${d}/mb-h52"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#gamma' --base-ref refs/drift/tail)"; rc=$?
check_eq "a merge base whose file equals the base ref's is exact (0)" "0" "${rc}"
check_eq "without a NOTE" "0" "$(grep -c '^NOTE' <<<"${out}")"
# Hunks that disagree with the file the merge base names are not trusted
# either. Here the merge base is the base ref, so only the hunk check sees
# it. Wrong: removed lines not compared (0, the hunk's context still
# matches); context lines not compared (0, the removed line still matches).
git -C "${base}" rev-parse refs/drift/line4 >"${d}/mb-h52"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L1-L1 --base-ref refs/drift/line4)"; rc=$?
check_eq "a removed line that differs from the file is not trusted (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #52 src/lib.rs: a hunk does not match refs/drift/line4" "${out}"
git -C "${base}" rev-parse refs/drift/line2 >"${d}/mb-h52"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L1-L1 --base-ref refs/drift/line2)"; rc=$?
check_eq "a context line that differs from the file is not trusted (66)" "66" "${rc}"
rm -f "${d}/mb-h52"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#gamma')"; rc=$?
check_eq "no drift: a symbol the PR does not touch still passes (0)" "0" "${rc}"

# Repeated rows. rows-mb is the pull request's merge base: src/t.rs holds
# array A (rows 2-21) and array B (rows 24-43), every row "    0,". rows-main
# adds 12 rows to A, so B runs 35-56 there and base line 30 is inside A,
# with every line of the hunk still equal to the line at its number.
rows_file() {
  local i
  printf 'const A: [u8; %s] = [\n' "$1"
  for ((i = 0; i < $1; i++)); do printf '    0,\n'; done
  printf '];\nconst B: [u8; 20] = [\n'
  for ((i = 0; i < 20; i++)); do printf '    0,\n'; done
  printf '];\n'
}
git -C "${base}" checkout -q -b rows
rows_file 20 >"${base}/src/t.rs"
git -C "${base}" add src/t.rs
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -m rows-mb
git -C "${base}" update-ref refs/rows/mb HEAD
rows_file 32 >"${base}/src/t.rs"
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -am rows-main
git -C "${base}" update-ref refs/rows/main HEAD
git -C "${base}" checkout -q -
# The pull request changes base line 30, inside B on its merge base.
patch_rows=('@@ -27,7 +27,7 @@' '     0,' '     0,' '     0,' '-    0,' '+    1,' '     0,' '     0,' '     0,')
# Wrong: line equality as the drift test (0, every hunk line matches on
# rows-main); the merge base asked for but its file not compared (0).
d="$(new_case rows_drift)"
open_pr "${d}" 60
pr_patch "${d}" 60 src/t.rs "${patch_rows[@]}"
git -C "${base}" rev-parse refs/rows/mb >"${d}/mb-h60"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#B' --base-ref refs/rows/main)"; rc=$?
check_eq "a shift over repeated rows is drift (66)" "66" "${rc}"
check_contains "and names the file and the reason" "NOTE  #60 src/t.rs: changed on refs/rows/main since the pull request forked" "${out}"
check_contains "asking the compare endpoint for the PR's base branch and head" "main...h60" "$(cat "${d}/calls-compare-h60")"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#B' --base-ref refs/rows/mb)"; rc=$?
check_eq "the same PR against its own base collides on B (66)" "66" "${rc}"
check_eq "without a NOTE" "0" "$(grep -c '^NOTE' <<<"${out}")"
# A merge base equal to the base ref and a hunk outside the entry passes.
# Wrong: every entry compared as the whole file (66). The merge base is
# asked once per pull request. Wrong: once per entry (2 calls).
rm -f "${d}/calls-compare-h60"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#A,src/t.rs:L1-L3' --base-ref refs/rows/mb)"; rc=$?
check_eq "an exact merge base and a hunk outside the entry pass (0)" "0" "${rc}"
check_eq "after one compare call for two entries" "1" "$(wc -l <"${d}/calls-compare-h60" | tr -d ' ')"
# A bare entry needs no merge base. Wrong: asked for every pull request.
rm -f "${d}/calls-compare-h60"
out="$(run_base "${d}" --issue 42 --paths src/t.rs --base-ref refs/rows/mb)"; rc=$?
check_eq "a bare entry on the same PR collides (66)" "66" "${rc}"
check_eq "without a compare call" "no" "$([[ -e "${d}/calls-compare-h60" ]] && echo yes || echo no)"
# A response with no merge base in it. Wrong: an empty merge base used as a
# revision (git reads ':src/t.rs' from the index, 0 or a different NOTE).
: >"${d}/mb-h60"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#A' --base-ref refs/rows/mb)"; rc=$?
check_eq "a compare response naming no merge base falls back (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #60 src/t.rs: the compare endpoint named no merge base" "${out}"
git -C "${base}" rev-parse refs/rows/mb >"${d}/mb-h60"
# A merge base this checkout does not have is not compared by line. Wrong:
# its absence read as a different file (66, but with the drift NOTE).
d="$(new_case mb_missing)"
open_pr "${d}" 61
pr_patch "${d}" 61 src/t.rs "${patch_rows[@]}"
printf '%040d\n' 7 >"${d}/mb-h61"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#A' --base-ref refs/rows/mb)"; rc=$?
check_eq "a merge base missing locally falls back to the whole file (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #61 src/t.rs: merge base $(printf '%040d' 7) is not available locally" "${out}"
# A compare call that fails is could-not-ask. Wrong: the failure taken as a
# fallback to the whole file (66); the failure ignored (0).
d="$(new_case compare_down)"
open_pr "${d}" 62
pr_patch "${d}" 62 src/t.rs "${patch_rows[@]}"
touch "${d}/compare-down-h62"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#A' --base-ref refs/rows/mb)"; rc=$?
check_eq "a failed merge-base lookup exits 69" "69" "${rc}"
check_contains "and refuses to call it clean" "refusing to report a clean answer" "${out}"
# A listing with no head commit cannot ask. Wrong: the compare URL built
# from empty fields (69, or a lookup of the wrong commits).
d="$(new_case no_head_oid)"
printf '[{"number":64,"title":"in flight","state":"OPEN","body":"","headRefName":"p","updatedAt":"2026-09-01T00:00:00Z","mergedAt":null,"closedAt":null}]\n' >"${d}/prs.json"
pr_patch "${d}" 64 src/t.rs "${patch_rows[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/t.rs#A' --base-ref refs/rows/mb)"; rc=$?
check_eq "a PR listed with no head commit falls back to the whole file (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #64 src/t.rs: the listing gives no base branch or head commit" "${out}"

# Added, removed and unanchored hunks. thirty.rs has lines "line 1" to
# "line 30" on origin/main, which is also the merge base in these cases, so
# only the hunks decide.
for i in $(seq 1 30); do printf 'line %s\n' "${i}"; done >"${base}/src/thirty.rs"
git -C "${base}" add src/thirty.rs
git -C "${base}" -c user.email=t@example.invalid -c user.name=t commit -q -m thirty
git -C "${base}" update-ref refs/remotes/origin/main HEAD
# A pull request that adds the file. Wrong: the hunk's base lines 0 and 1
# compared against the range (0); only removed files covered (0).
d="$(new_case file_added)"
open_pr "${d}" 65
jq -n --arg p "$(printf '%s\n' '@@ -0,0 +1,3 @@' '+a' '+b' '+c')" \
  '[{filename: "src/thirty.rs", status: "added", patch: $p}]' >"${d}/files-65.json"
out="$(run_base "${d}" --issue 42 --paths src/thirty.rs:L5-L20)"; rc=$?
check_eq "a PR adding a file main already has collides as the whole file (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #65 src/thirty.rs: the pull request adds or deletes this file" "${out}"
# Wrong: the status field read and the hunk header not (the NOTE then names
# the unanchored hunk instead).
pr_patch "${d}" 65 src/thirty.rs '@@ -0,0 +1,3 @@' '+a' '+b' '+c'
out="$(run_base "${d}" --issue 42 --paths src/thirty.rs:L5-L20)"; rc=$?
check_eq "a hunk with an empty old side collides as the whole file (66)" "66" "${rc}"
check_contains "and says the PR adds it" "adds or deletes this file" "${out}"
# A pull request that deletes the file it had at 3 lines. Wrong: the removed
# lines 1-3 compared against the range (0); only added files covered (0).
d="$(new_case file_removed)"
open_pr "${d}" 66
jq -n --arg p "$(printf '%s\n' '@@ -1,3 +0,0 @@' '-line 1' '-line 2' '-line 3')" \
  '[{filename: "src/thirty.rs", status: "removed", patch: $p}]' >"${d}/files-66.json"
out="$(run_base "${d}" --issue 42 --paths src/thirty.rs:L20-L25)"; rc=$?
check_eq "a PR deleting the file collides as the whole file (66)" "66" "${rc}"
check_contains "and says so" "adds or deletes this file" "${out}"
# The hunk alone says so when the status field is missing. Wrong: the status
# field read and the hunk header not (0).
pr_patch "${d}" 66 src/thirty.rs '@@ -1,3 +0,0 @@' '-line 1' '-line 2' '-line 3'
out="$(run_base "${d}" --issue 42 --paths src/thirty.rs:L20-L25)"; rc=$?
check_eq "a hunk with an empty new side collides as the whole file (66)" "66" "${rc}"
# A zero-context insertion has no line to check its numbering against.
# Wrong: no such check (0, base lines 13, 30 and 31 are outside L1-L10); the
# check made for the patch as a whole (0, the first hunk has lines).
d="$(new_case zero_context)"
open_pr "${d}" 67
pr_patch "${d}" 67 src/thirty.rs '@@ -12,3 +12,3 @@' ' line 12' '-line 13' '+line 13b' ' line 14' '@@ -30,0 +31,1 @@' '+new'
out="$(run_base "${d}" --issue 42 --paths src/thirty.rs:L1-L10)"; rc=$?
check_eq "a hunk with no context or removed line collides as the whole file (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #67 src/thirty.rs: a hunk has no context or removed line to check" "${out}"

# An empty patch is no patch. Wrong: an empty patch read as no hunks (0).
d="$(new_case patch_empty)"
open_pr "${d}" 68
printf '[{"filename":"src/core.rs","previous_filename":"src/lib.rs","status":"renamed","patch":""}]\n' >"${d}/files-68.json"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L3-L6)"; rc=$?
check_eq "a renamed file with an empty patch collides as the whole file (66)" "66" "${rc}"
check_contains "and says why" "no patch" "${out}"
printf '[{"filename":"src/lib.rs","status":"modified","patch":""}]\n' >"${d}/files-68.json"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L3-L6)"; rc=$?
check_eq "so does a modified file with an empty patch (66)" "66" "${rc}"

# GitHub's files endpoint lists at most 3000 files. Wrong: no cap (0, the
# entries are not in the list); the cap read as more than 3000 (0).
d="$(new_case files_cap)"
open_pr "${d}" 69
for i in $(seq 1 3000); do printf 'src/pad_%s.rs\n' "${i}"; done >"${d}/files-69.txt"
out="$(run_base "${d}" --issue 42 --paths 'src/mine.rs,src/lib.rs#alpha')"; rc=$?
check_eq "a PR listing 3000 files collides on every entry (66)" "66" "${rc}"
check_contains "and says so" "NOTE  #69 lists 3000 files, the endpoint's limit" "${out}"
check_contains "naming each entry" "src/mine.rs, src/lib.rs#alpha" "${out}"
sed -i.bak '3000d' "${d}/files-69.txt"
out="$(run_base "${d}" --issue 42 --paths 'src/mine.rs,src/lib.rs#alpha')"; rc=$?
check_eq "one fewer is a complete list (0)" "0" "${rc}"

# Renames: an entry naming the old path matches, and the patch is numbered on
# the old file. Wrong: only .filename matched (0 for both); a rename compared
# as the whole file (the gamma case collides).
d="$(new_case rename)"
open_pr "${d}" 53
jq -n --arg p "$(printf '%s\n' "${patch_a[@]}")" \
  '[{filename: "src/core.rs", previous_filename: "src/lib.rs", status: "renamed", patch: $p}]' >"${d}/files-53.json"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs)"; rc=$?
check_eq "a PR renaming the file collides on the old path (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#alpha')"; rc=$?
check_eq "and on an old-path symbol whose body it edits (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#gamma')"; rc=$?
check_eq "but not on one it leaves alone (0)" "0" "${rc}"
out="$(run_base "${d}" --issue 42 --paths src/core.rs)"; rc=$?
check_eq "the new path still collides (66)" "66" "${rc}"

# DISPATCH_PATHS is written by hand. Wrong: no trim (0, " docs/guide.md"
# names no file); trimmed but an empty entry skipped (0 for the second).
d="$(new_case trim_entries)"
open_pr "${d}" 54
pr_patch "${d}" 54 docs/guide.md "${patch_e[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/x.rs, docs/guide.md')"; rc=$?
check_eq "an entry after a comma and a space is trimmed (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths ' docs/guide.md#usage ')"; rc=$?
check_eq "a symbol entry is trimmed too (66)" "66" "${rc}"
out="$(run_base "${d}" --issue 42 --paths 'src/x.rs, ,docs/guide.md')"; rc=$?
check_eq "an entry empty after trimming is a usage error (64)" "64" "${rc}"
# Wrong: split with read -a, which drops a trailing empty field (66 for the
# first) and stops at a newline (0 for the newline case, docs/guide.md unread).
for value in 'docs/guide.md,' ',docs/guide.md' 'src/x.rs,,docs/guide.md'; do
  out="$(run_base "${d}" --issue 42 --paths "${value}")"; rc=$?
  check_eq "'${value}' has an empty entry, a usage error (64)" "64" "${rc}"
done
d="$(new_case paths_newline)"
open_pr "${d}" 55
pr_patch "${d}" 55 docs/guide.md "${patch_e[@]}"
out="$(run_base "${d}" --issue 42 --paths $'src/x.rs\ndocs/guide.md')"; rc=$?
check_eq "a newline in --paths is a usage error (64)" "64" "${rc}"
check_contains "and says so" "newline or carriage return" "${out}"
out="$(run_base "${d}" --issue 42 --paths $'src/x.rs\rdocs/guide.md')"; rc=$?
check_eq "so is a carriage return (64)" "64" "${rc}"
check_eq "before any gh call" "0" "$(find "${d}" -name 'calls-*' | wc -l | tr -d ' ')"

# --- a diff that cannot be read is never a pass ---------------------------
# Wrong: patches fetched in a second call whose failure falls back to the
# filename list (0).
d="$(new_case patch_down)"
open_pr "${d}" 44
pr_patch "${d}" 44 src/lib.rs "${patch_a[@]}"
touch "${d}/patch-down-44"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L3-L6)"; rc=$?
check_eq "a diff fetch failure exits 69" "69" "${rc}"
check_contains "and refuses to call it clean" "refusing to report a clean answer" "${out}"

# A file the endpoint returns with no patch (a diff too large to inline) is
# compared as the whole file. Wrong: an empty patch read as no hunks (0).
d="$(new_case patch_absent)"
open_pr "${d}" 45
printf '[{"filename":"src/lib.rs"}]\n' >"${d}/files-45.json"
out="$(run_base "${d}" --issue 42 --paths src/lib.rs:L3-L6)"; rc=$?
check_eq "a file with no patch collides as the whole file (66)" "66" "${rc}"
check_contains "and says why" "no patch" "${out}"

# A base the guard cannot read is could-not-ask.
d="$(new_case base_missing)"
open_pr "${d}" 46
pr_patch "${d}" 46 src/lib.rs "${patch_a[@]}"
out="$(run_base "${d}" --issue 42 --paths 'src/lib.rs#alpha' --base-ref refs/heads/no-such-ref)"; rc=$?
check_eq "an unreadable base ref exits 69" "69" "${rc}"

# --- an entry that does not parse is usage, before any query -------------
d="$(new_case bad_entries)"
open_pr "${d}" 47
pr_patch "${d}" 47 src/lib.rs "${patch_a[@]}"
for entry in 'src/lib.rs:L10' 'src/lib.rs#' 'src/lib.rs:L80-L10' 'src/lib.rs:L0-L3' '#alpha' 'src/lib.rs:10-80' 'src/lib.rs#al pha'; do
  out="$(run_base "${d}" --issue 42 --paths "${entry}")"; rc=$?
  check_eq "'${entry}' is a usage error (64)" "64" "${rc}"
done

# --- usage -------------------------------------------------------------
d="$(new_case usage)"
out="$(run_in "${d}" "${GUARD}")"; rc=$?
check_eq "no --issue is a usage error (64)" "64" "${rc}"
out="$(run_in "${d}" "${GUARD}" --issue abc)"; rc=$?
check_eq "a non-numeric issue is a usage error (64)" "64" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
