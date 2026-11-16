#!/usr/bin/env bash
# Repository-wide flake ledger (ADR-4034, issue #4044).
#
# assert-green-head.sh records a failure signature per pull request and head,
# but that state is per session and per epic, so the same flaky test ejecting
# three different pull requests reads as three unrelated reds. This sweep
# keeps one ledger for the repository, on GitHub issues:
#
#   1. Lists the failed ci.yml runs of the last 24 h on merge_group and on
#      push to main.
#   2. Fetches each failed job's log and extracts test identifiers. The
#      ledger is repository-wide, so an identifier names the crate as well
#      as the test: two crates often define a test of the same path.
#      - nextest's result line `FAIL [ time] (n/m) <binary-id> <test>` (also
#        TIMEOUT, ABORT and SIG*) gives `<binary-id> <test>`, for example
#        `ravel-codec::fuzz_mutation arbitrary_bytes_never_panic`.
#      - libtest's lines, matched with the grep assert-green-head.sh uses
#        (read out of that script at run time, so the two cannot diverge),
#        carry no crate. A test path a nextest line in the same job already
#        names is dropped; any other is qualified with the failed job's name
#        as `[<job name>] <test>`.
#      A failed job whose non-empty log yields no identifier (a compile
#      error, a step that is not a test runner, an output format this does
#      not read) prints one line naming the run, job id and job name, and is
#      counted in the summary line: the ejection is in no issue's ledger.
#   3. Files or updates ONE issue per identifier, labelled flaky-test, found
#      by the deterministic title "flaky-test: <identifier>". The issue body
#      carries one ledger line per failed run that named the identifier; a
#      run already in the ledger is not counted again, so overlapping sweep
#      windows count each run once. A closed issue for the identifier is
#      reopened rather than duplicated.
#   4. A failure within 14 days of another failure of the same identifier
#      adds the label flake-quarantine-due.
#   5. Fails when an open flake-quarantine-due issue has been due for more
#      than one working day and no open pull request references it.
#
# Times are UTC. A working day is Monday to Friday; "due for more than one
# working day" counts only the seconds of the due interval that fall on a
# weekday, and the interval starts at the issue's "- quarantine-due at=" line
# (written when the sweep adds the label), else at the issue's creation.
#
# Usage: ci-flake-sweep.sh
#
# Environment:
#   GH_REPO                       owner/name, when not run inside a checkout
#   CI_FLAKE_SWEEP_NOW            ISO-8601 UTC time to treat as now (tests)
#   CI_FLAKE_SWEEP_RETRY_DELAY_S  seconds between gh read retries (default 5)
#
# Exit codes:
#   0   swept; no overdue flake-quarantine-due issue (the summary line still
#       counts failed jobs no identifier was read from)
#   1   an open flake-quarantine-due issue is overdue with no pull request
#   2   a gh call failed; nothing can be said (this is not a clean run)
#   64  usage
#   70  could not run, and the message says which of three causes:
#       - jq is not installed;
#       - assert-green-head.sh, or its one identifier grep, could not be
#         located (the anchor moved);
#       - the decision for one identifier failed, most likely an issue body
#         whose hand-edited "- failure run=... at=..." line does not parse
#         as a date.
set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL_FLAKY="flaky-test"
LABEL_DUE="flake-quarantine-due"
TITLE_PREFIX="flaky-test: "
WINDOW_S=86400
REPEAT_WINDOW_S=$((14 * 86400))

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h | --help)
      awk 'NR == 1 { next } /^#/ { sub(/^# ?/, ""); print; next } { exit }' "$0"
      exit 0
      ;;
    *)
      echo "ci-flake-sweep.sh: unknown argument: $1" >&2
      exit 64
      ;;
  esac
fi

command -v jq >/dev/null 2>&1 || {
  echo "ci-flake-sweep.sh: jq is required" >&2
  exit 70
}

# The identifier grep is assert-green-head.sh's own, read out of its single
# `tests="$(grep -aoE '...'` line. That script runs gh at top level, so it
# cannot be sourced; a copied regex would drift from it.
AGH="${SELF_DIR}/assert-green-head.sh"
anchor="tests=\"\$(grep -aoE '"
if [[ ! -f "${AGH}" ]]; then
  echo "ci-flake-sweep.sh: cannot find ${AGH} to reuse its identifier grep" >&2
  exit 70
fi
anchor_count="$(grep -cF "${anchor}" "${AGH}")"
if [[ "${anchor_count}" != "1" ]]; then
  echo "ci-flake-sweep.sh: expected one identifier grep in ${AGH}, found ${anchor_count}" >&2
  exit 70
fi
pattern_line="$(grep -F "${anchor}" "${AGH}")"
ID_PATTERN="${pattern_line#*"${anchor}"}"
ID_PATTERN="${ID_PATTERN%%"' <<<"*}"
case "${ID_PATTERN}" in
  *FAILED* | *panicked*) ;;
  *)
    echo "ci-flake-sweep.sh: the grep read from ${AGH} does not look like the identifier grep: ${ID_PATTERN}" >&2
    exit 70
    ;;
esac

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

if [[ -n "${CI_FLAKE_SWEEP_NOW:-}" ]]; then
  now="$(jq -nr --arg n "${CI_FLAKE_SWEEP_NOW}" '$n | fromdateiso8601' 2>/dev/null)" || now=""
  if [[ -z "${now}" ]]; then
    echo "ci-flake-sweep.sh: CI_FLAKE_SWEEP_NOW is not YYYY-MM-DDTHH:MM:SSZ: ${CI_FLAKE_SWEEP_NOW}" >&2
    exit 64
  fi
else
  now="$(jq -nr 'now | floor')"
fi
since_iso="$(jq -nr --argjson n "${now}" --argjson w "${WINDOW_S}" '$n - $w | todate')"

# gh with up to three attempts. stdout is the data; stderr goes to a file and
# is printed only when every attempt failed, so a warning never lands in JSON.
gh_retry() {
  local attempt=1 rc delay="${CI_FLAKE_SWEEP_RETRY_DELAY_S:-5}"
  while :; do
    rc=0
    gh "$@" </dev/null >"${tmpdir}/gh.out" 2>"${tmpdir}/gh.err" || rc=$?
    if ((rc == 0)); then
      cat "${tmpdir}/gh.out"
      return 0
    fi
    if ((attempt >= 3)); then
      echo "ci-flake-sweep.sh: 'gh $*' failed (exit ${rc}): $(tail -n 2 "${tmpdir}/gh.err" | tr '\n' ' ')" >&2
      return "${rc}"
    fi
    attempt=$((attempt + 1))
    sleep "${delay}"
  done
}

# Writes are tried once, through gh_once: a create that failed after the
# server stored it would be duplicated by a retry, and the next sweep re-reads
# the list anyway. The exception is `issue edit`, which goes through gh_retry:
# it replaces the whole body and adds a label, so a repeat changes nothing.
gh_once() {
  local rc=0
  gh "$@" </dev/null >"${tmpdir}/gh.out" 2>"${tmpdir}/gh.err" || rc=$?
  if ((rc != 0)); then
    echo "ci-flake-sweep.sh: 'gh $*' failed (exit ${rc}): $(tail -n 2 "${tmpdir}/gh.err" | tr '\n' ' ')" >&2
    return "${rc}"
  fi
  cat "${tmpdir}/gh.out"
}

could_not_ask() {
  echo "ci-flake-sweep.sh: could not ask GitHub; exiting 2 (this is not a clean run)" >&2
  exit 2
}

# Turns the grep's matches into libtest test paths. `error[E...]` and a bare
# `assertion failed` name no test, a thread name outside [A-Za-z0-9_:] (a
# runtime worker) or `main` is not one either, and the `FAILED [...]`
# alternative matches no nextest output (nextest prints `FAIL [`).
normalize_ids() {
  LC_ALL=C sed -n \
    -e 's/^test \([A-Za-z0-9_:]*\) \.\.\. FAILED$/\1/p' \
    -e "s/^thread '\{0,1\}\([A-Za-z0-9_:]*\)'\{0,1\}\( ([0-9]*)\)\{0,1\} panicked\$/\1/p" |
    awk 'NF && $0 != "main"' | LC_ALL=C sort -u
}

# nextest's per-test result line, the one line that names the binary (crate)
# as well as the test. A `TRY n` prefix or a `LEAK-` one leaves the match as is.
NEXTEST_PATTERN='(FAIL|TIMEOUT|ABORT|SIG[A-Z0-9]+) \[[^]]*\]( \([0-9]+/[0-9]+\))? [A-Za-z0-9_:/.-]+ [A-Za-z0-9_:]+'
nextest_ids() {
  LC_ALL=C sed -E 's/^[^]]*\]( \([0-9]+\/[0-9]+\))? ([^ ]+) ([^ ]+)$/\2 \3/' | LC_ALL=C sort -u
}

esc="$(printf '\033')"
labels_ready=0
occurrences="${tmpdir}/occurrences.tsv"
: >"${occurrences}"

# --- 1-2. failed runs and the identifiers in their logs -----------------

runs="${tmpdir}/runs.tsv"
: >"${runs}"
for event in merge_group push; do
  if [[ "${event}" == "push" ]]; then
    branch_args=(--branch main)
  else
    branch_args=()
  fi
  # ${arr[@]+...} keeps bash 3.2 from calling an empty array unbound.
  gh_retry run list --workflow ci.yml --event "${event}" --status failure \
    ${branch_args[@]+"${branch_args[@]}"} --created ">=${since_iso}" --limit 200 \
    --json databaseId,createdAt,event,url >"${tmpdir}/runs.json" || could_not_ask
  jq -r --argjson since "$((now - WINDOW_S))" \
    '.[] | select((.createdAt | sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601) >= $since)
      | [.databaseId, (.createdAt | sub("\\.[0-9]+Z$"; "Z")), .event, .url] | @tsv' \
    "${tmpdir}/runs.json" >>"${runs}" 2>"${tmpdir}/jq.err" || {
    echo "ci-flake-sweep.sh: gh run list returned unreadable JSON: $(head -n 1 "${tmpdir}/jq.err")" >&2
    could_not_ask
  }
done

run_count=0
empty_logs=0
unread_jobs=0
while IFS=$'\t' read -r run_id created event url; do
  [[ -z "${run_id}" ]] && continue
  run_count=$((run_count + 1))
  gh_retry run view "${run_id}" --json jobs >"${tmpdir}/jobs.json" || could_not_ask
  failed_jobs="$(jq -r '.jobs[]? | select(.conclusion == "failure") | [.databaseId, (.name // "")] | @tsv' "${tmpdir}/jobs.json" 2>/dev/null)" || {
    echo "ci-flake-sweep.sh: gh run view ${run_id} returned unreadable JSON" >&2
    could_not_ask
  }
  : >"${tmpdir}/run-ids.txt"
  while IFS=$'\t' read -r job_id job_name; do
    [[ -z "${job_id}" ]] && continue
    gh_retry run view --job "${job_id}" --log-failed >"${tmpdir}/job.log" || could_not_ask
    if [[ ! -s "${tmpdir}/job.log" ]]; then
      echo "ci-flake-sweep.sh: run ${run_id} job ${job_id}: empty failed log; no identifier read from it" >&2
      empty_logs=$((empty_logs + 1))
      continue
    fi
    # Colour codes first, as assert-green-head.sh does: the SGR sequence ends
    # in a letter and would otherwise glue itself to the token.
    LC_ALL=C sed "s/${esc}\\[[0-9;]*[A-Za-z]//g" "${tmpdir}/job.log" >"${tmpdir}/job.clean"
    { grep -aoE "${NEXTEST_PATTERN}" "${tmpdir}/job.clean" || true; } | nextest_ids >"${tmpdir}/job-nextest.txt"
    { grep -aoE "${ID_PATTERN}" "${tmpdir}/job.clean" || true; } | normalize_ids >"${tmpdir}/job-libtest.txt"
    {
      cat "${tmpdir}/job-nextest.txt"
      JOB_NAME="${job_name}" awk '
        FILENAME == ARGV[1] { sub(/^[^ ]* /, ""); named[$0] = 1; next }
        !($0 in named) { print "[" ENVIRON["JOB_NAME"] "] " $0 }
      ' "${tmpdir}/job-nextest.txt" "${tmpdir}/job-libtest.txt"
    } >"${tmpdir}/job-ids.txt"
    if [[ ! -s "${tmpdir}/job-ids.txt" ]]; then
      echo "ci-flake-sweep.sh: run ${run_id} job ${job_id} (${job_name}): no identifier read from a non-empty failed log; this failure is in no issue's ledger"
      unread_jobs=$((unread_jobs + 1))
      continue
    fi
    cat "${tmpdir}/job-ids.txt" >>"${tmpdir}/run-ids.txt"
  done <<<"${failed_jobs}"
  LC_ALL=C sort -u "${tmpdir}/run-ids.txt" | while IFS= read -r id; do
    printf '%s\t%s\t%s\t%s\t%s\n' "${id}" "${run_id}" "${created}" "${event}" "${url}"
  done >>"${occurrences}"
done <"${runs}"

# --- 3-4. one issue per identifier ---------------------------------------

ids="$(cut -f1 "${occurrences}" | LC_ALL=C sort -u)"
if [[ -n "${ids}" ]]; then
  gh_retry issue list --label "${LABEL_FLAKY}" --state all --limit 1000 \
    --json number,title,body,labels,state >"${tmpdir}/issues.json" || could_not_ask
  jq -e 'type == "array"' "${tmpdir}/issues.json" >/dev/null 2>&1 || {
    echo "ci-flake-sweep.sh: gh issue list returned unreadable JSON" >&2
    could_not_ask
  }
fi

ensure_labels() {
  ((labels_ready == 1)) && return 0
  gh_once label create "${LABEL_FLAKY}" --force --color D93F0B \
    --description "A test that failed a ci.yml run on merge_group or main (ci-flake-sweep.sh)" >/dev/null || return 1
  gh_once label create "${LABEL_DUE}" --force --color B60205 \
    --description "Failed twice within 14 days: quarantine or fix within one working day" >/dev/null || return 1
  labels_ready=1
}

# One jq program decides each identifier: which issue, which runs are new,
# whether the repeat rule fires, and the body to write.
# shellcheck disable=SC2016
decide_filter='
def ts: sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601;
def ledger: (. // "") | gsub("\r"; "") | split("\n")
  | map(capture("^- failure run=(?<run>[0-9]+) at=(?<at>[0-9TZ:-]+)")) ;
($prefix + $id) as $title
| [ $issues[0][] | select(.title | startswith($title))
    | select((.title | length) == ($title | length) or (.title[($title | length):($title | length) + 1] == " ")) ]
| (map(select(.state == "OPEN")) + map(select(.state != "OPEN")) | .[0]) as $issue
| ($issue.body | ledger) as $old
| ($old | map(.run)) as $seen
| [ $occ[] | select((.run | tostring) as $r | any($seen[]; . == $r) | not) ] as $new
| ($old + ($new | map({run: (.run | tostring), at: .at}))) as $all
| ([ $issue.labels[]?.name ] | index($due) != null) as $has_due
| ([ $new[] as $n | $all[] | select(.run != ($n.run | tostring))
     | select(((.at | ts) - ($n.at | ts)) | (if . < 0 then -. else . end) <= $repeat) ] | length > 0) as $repeat_hit
| ($repeat_hit and ($has_due | not)) as $add_due
| ([ $new[] | "- failure run=\(.run) at=\(.at) event=\(.event) \(.url)" ]
   + (if $add_due then ["- quarantine-due at=\($now | todate)"] else [] end)) as $lines
| if ($new | length) == 0 then {action: "none", number: $issue.number}
  elif $issue == null then
    {action: "create", title: $title, add_due: $add_due,
     body: ("Test `\($id)` failed in a ci.yml run on merge_group or on push to main.\n\n"
       + "The identifier is the nextest binary id and test name, or, for a job whose log had only libtest output, "
       + "the name of the failed job in brackets and the test path.\n\n"
       + "Filed by `scripts/guards/ci-flake-sweep.sh` (ADR-4034). A second failure within 14 days adds `\($due)`; "
       + "a due issue needs an open pull request that references it (a fix, or an `#[ignore]` whose reason names flaky and cites this issue) "
       + "within one working day, or the sweep run fails.\n\n"
       + "The sweep appends one ledger line per failed run and reads them back: leave the lines below as they are.\n\n"
       + ($lines | join("\n")) + "\n")}
  else
    {action: "update", number: $issue.number, reopen: ($issue.state != "OPEN"), add_due: $add_due,
     body: (($issue.body // "" | sub("\n*$"; "")) + "\n" + ($lines | join("\n")) + "\n")}
  end
'

# An identifier holds a space, so the loop reads lines, on fd 3 so that no
# command in the body can consume the list.
while IFS= read -r id <&3; do
  [[ -z "${id}" ]] && continue
  ID="${id}" awk -F'\t' '$1 == ENVIRON["ID"] { print $2 "\t" $3 "\t" $4 "\t" $5 }' "${occurrences}" |
    jq -R -s 'split("\n") | map(select(length > 0) | split("\t") | {run: (.[0] | tonumber), at: .[1], event: .[2], url: .[3]})' \
      >"${tmpdir}/occ.json"
  jq -n --arg id "${id}" --arg prefix "${TITLE_PREFIX}" --arg due "${LABEL_DUE}" \
    --argjson now "${now}" --argjson repeat "${REPEAT_WINDOW_S}" \
    --slurpfile issues "${tmpdir}/issues.json" --slurpfile occs "${tmpdir}/occ.json" \
    '$occs[0] as $occ | '"${decide_filter}" >"${tmpdir}/decision.json" 2>"${tmpdir}/jq.err" || {
    echo "ci-flake-sweep.sh: could not decide for ${id} (check its issue body's ledger lines): $(head -n 1 "${tmpdir}/jq.err")" >&2
    exit 70
  }
  action="$(jq -r .action "${tmpdir}/decision.json")"
  add_due="$(jq -r '.add_due // false' "${tmpdir}/decision.json")"
  jq -r '.body // ""' "${tmpdir}/decision.json" >"${tmpdir}/body.md"
  case "${action}" in
    none)
      echo "${id}: every run already in #$(jq -r .number "${tmpdir}/decision.json")"
      ;;
    create)
      ensure_labels || could_not_ask
      label_args=(--label "${LABEL_FLAKY}")
      [[ "${add_due}" == "true" ]] && label_args+=(--label "${LABEL_DUE}")
      created="$(gh_once issue create --title "$(jq -r .title "${tmpdir}/decision.json")" \
        "${label_args[@]}" --body-file "${tmpdir}/body.md")" || could_not_ask
      echo "${id}: filed ${created}"
      ;;
    update)
      number="$(jq -r .number "${tmpdir}/decision.json")"
      if [[ "$(jq -r .reopen "${tmpdir}/decision.json")" == "true" ]]; then
        gh_once issue reopen "${number}" >/dev/null || could_not_ask
      fi
      edit_args=(--body-file "${tmpdir}/body.md")
      if [[ "${add_due}" == "true" ]]; then
        ensure_labels || could_not_ask
        edit_args+=(--add-label "${LABEL_DUE}")
      fi
      gh_retry issue edit "${number}" "${edit_args[@]}" >/dev/null || could_not_ask
      echo "${id}: updated #${number}$([[ "${add_due}" == "true" ]] && echo " (now ${LABEL_DUE})")"
      ;;
  esac
done 3<<<"${ids}"

summary() {
  echo "ci-flake-sweep.sh: ${run_count} failed runs swept, $(grep -c . <<<"${ids}") test identifiers," \
    "${unread_jobs} failed jobs with no identifier read from a non-empty log, ${empty_logs} with an empty failed log"
}

# --- 5. overdue quarantines ---------------------------------------------

gh_retry issue list --label "${LABEL_DUE}" --state open --limit 1000 \
  --json number,title,body,createdAt >"${tmpdir}/due.json" || could_not_ask

# shellcheck disable=SC2016
overdue="$(jq -r --argjson now "${now}" '
def ts: sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601;
def weekday_seconds($a; $b):
  if $b <= $a then 0 else
    reduce range(($a / 86400 | floor); ($b / 86400 | floor) + 1) as $d (0;
      ((($d + 4) % 7) as $w
       | if $w >= 1 and $w <= 5
         then . + ([([($d + 1) * 86400, $b] | min) - ([$d * 86400, $a] | max), 0] | max)
         else . end))
  end;
.[]
| ([(.body // "") | gsub("\r"; "") | split("\n")[] | capture("^- quarantine-due at=(?<at>[0-9TZ:-]+)") | .at] | last) as $marked
| (($marked // .createdAt) | ts) as $since
| select(weekday_seconds($since; $now) > 86400)
| "\(.number)\t\(.title)"
' "${tmpdir}/due.json" 2>"${tmpdir}/jq.err")" || {
  echo "ci-flake-sweep.sh: gh issue list returned unreadable JSON: $(head -n 1 "${tmpdir}/jq.err")" >&2
  could_not_ask
}

if [[ -z "${overdue}" ]]; then
  echo "ci-flake-sweep.sh: no ${LABEL_DUE} issue is overdue"
  summary
  exit 0
fi

gh_retry pr list --state open --limit 1000 --json number,title,body >"${tmpdir}/prs.json" || could_not_ask
jq -e 'type == "array"' "${tmpdir}/prs.json" >/dev/null 2>&1 || {
  echo "ci-flake-sweep.sh: gh pr list returned unreadable JSON" >&2
  could_not_ask
}

found=0
while IFS=$'\t' read -r number title; do
  [[ -z "${number}" ]] && continue
  refs="$(jq -r --arg n "${number}" \
    '[.[] | select(((.title // "") + "\n" + (.body // "")) | test("(#|/issues/)" + $n + "([^0-9]|$)")) | "#\(.number)"] | join(" ")' \
    "${tmpdir}/prs.json")"
  if [[ -n "${refs}" ]]; then
    echo "#${number} ${title}: due, referenced by ${refs}"
  else
    echo "OVERDUE: #${number} ${title}: ${LABEL_DUE} for more than one working day and no open pull request references it" >&2
    found=1
  fi
done <<<"${overdue}"

summary
exit "${found}"
