#!/usr/bin/env bash
# Refuse to dispatch work that someone is already doing.
#
# This asks the question BEFORE a branch exists: given an issue number and
# the files a task is predicted to touch, is there already a pull request
# that addresses that issue, or one in flight over those files? It is the
# pre-dispatch half of the duplicate-work problem. The post-hoc half, two
# pull requests that already exist and turn out to make the same change,
# is scripts/guards/check-duplicate-work.sh, which compares patch-ids;
# neither can answer the other's question, because a task about to be
# dispatched has no diff to hash yet.
#
# The file list per candidate comes from the paginated pulls/<n>/files
# endpoint rather than `gh pr list --json files`, which caps at 100 entries
# and would report a wide pull request as touching none of the paths asked
# about: a false clean, which is the only wrong answer that costs anything
# here. That endpoint returns at most 3000 files, so a pull request listing
# 3000 or more may touch more than it lists and is taken as colliding with
# every entry, with a NOTE.
#
# A --paths entry takes one of three forms:
#   path          the whole file: any change the pull request makes to it
#                 collides.
#   path:L10-L80  a base line range: collides only when one of the pull
#                 request's patch hunks for that file changes a base line
#                 inside it.
#   path#symbol   a definition on the base: collides only when a hunk changes
#                 a base line inside it.
# Entries are separated by commas. Each entry is trimmed of leading and
# trailing whitespace; an entry empty after trimming, including one left by
# a leading, trailing or doubled comma, is a usage error, and so is a --paths
# value containing a newline or carriage return. A changed base line is a removed or
# replaced line; a pure insertion counts against the base lines on both sides
# of it; context lines do not count. A range or symbol entry whose file is
# not on the base, and a range that starts past the base file's last line,
# are compared as the whole file, with a NOTE.
#
# The symbol anchor is plain text, not a parser, and applies to .rs files
# (the Rust forms) and .sh and .bash files (the shell forms). The Rust forms
# match a line that defines the name with fn, struct, enum, trait, mod,
# const, static, type or union (after pub and the async, const, unsafe,
# default or extern qualifiers), or an impl line that names it anywhere in
# its header; the shell forms match `name()` or `function name`. A symbol
# entry on any other file type, docs/ markdown aside, is compared as the
# whole file with a NOTE. The definition runs from the anchor line to its
# matching close brace; to the first semicolon outside brackets when no
# brace opens first; or, when braces never balance, to the line before the
# next definition at column 0. Braces in strings and comments are counted
# like any other, and a column-0 fn inside a Rust raw string or a column-0
# name() { inside a shell heredoc reads as the next definition, so each can
# cut a definition short. Doc comments and attributes above the anchor are
# not part of the definition. Every anchor that matches counts. A name not
# found on the base is compared as the whole file, with a NOTE.
#
# A .md file under docs/ is compared by heading section instead of by line:
# a line's section is its nearest preceding ATX heading outside fenced code,
# and an entry collides only when a hunk changes a line in one of the
# entry's sections. A fence closes only on a fence of the same character
# (backtick or tilde) at least as long as the one that opened it.
# path#symbol names a heading by its GitHub anchor (docs/x.md#usage for
# "## Usage") and covers its nested subsections; path:L10-L80 covers every
# section with a line in the range. A bare docs/ path names no section and
# keeps the whole-file rule.
#
# Hunks come from the patch field of the same paginated files endpoint, in
# the same call. A file the pull request renamed matches an entry naming
# either its old or its new path, and its patch is numbered on the old file.
# Symbols, headings and sections are read from --base-ref (default
# origin/main), while a pull request's hunks number lines on its own merge
# base. A range, symbol or section entry is compared by line only when all
# of these hold for the matching file; otherwise that file is compared as
# the whole file, with a NOTE naming the file and the reason:
#   - the file has a patch: one absent or empty (a diff too large to inline)
#     is not compared by line;
#   - the pull request neither adds nor deletes the file: status added or
#     removed, or a hunk header with an empty side (-0,0 or +0,0), is not;
#   - the merge base is known and present locally. It is read once per pull
#     request, and only when an entry needs it, from the compare endpoint
#     (repos/OWNER/REPO/compare/BASE...HEAD, BASE the pull request's base
#     branch and HEAD its head commit, .merge_base_commit.sha). A listing
#     with no base branch or head commit, a response naming no merge base,
#     or a merge base this checkout does not have (it is not fetched) each
#     fall back. A gh failure on that call exits 69;
#   - the file on the merge base (its old path, for a rename) is the same
#     blob as the entry's file on --base-ref. Any difference, wherever it is
#     in the file, means it changed since the pull request forked;
#   - every context and removed line of each hunk equals the base file's
#     line at the number its hunk header gives it, and every hunk has at
#     least one such line to check.
#
# Usage:
#   assert-no-duplicate-dispatch.sh --issue <n> [--paths a,b,c] [--paths d]
#                                   [--closed-days N] [--repo owner/name]
#                                   [--base-ref REF]
#
# Exit codes:
#   0   nothing overlapping; dispatch
#   64  usage, including a --paths entry that does not parse (path:L10 with
#       no end, a start of 0 or after the end, path# with no symbol, #symbol
#       with no path, or a path containing ':' or '#' in any other way), an
#       entry empty after trimming, or a --paths value containing a newline
#       or carriage return; each is refused before any gh call
#   65  an existing pull request already addresses the issue; skip and log
#   66  an OPEN, non-draft pull request is already touching the predicted
#       files, line ranges or definitions (drafts, merged and closed ones are
#       listed as context only)
#   69  the question could not be asked (gh failed after its retries, a pull
#       request list came back as long as its limit and may be truncated, a
#       pull request's files and patches or its merge base could not be
#       fetched, or --base-ref does not resolve); not the same as a clean
#       answer, and never report it as one
set -euo pipefail

issue=""
paths=()
closed_days=14
repo=""
base_ref="origin/main"

# Prints bare, range or symbol for an entry that parses; fails otherwise.
entry_form() {
  local e="$1"
  if [[ "${e}" == *"#"* ]]; then
    [[ "${e}" =~ ^[^#:]+#[A-Za-z0-9_-]+$ ]] || return 1
    echo symbol
  elif [[ "${e}" == *:* ]]; then
    [[ "${e}" =~ ^[^#:]+:L([1-9][0-9]*)-L([1-9][0-9]*)$ ]] || return 1
    ((BASH_REMATCH[1] <= BASH_REMATCH[2])) || return 1
    echo range
  else
    echo bare
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --issue) issue="${2:-}"; shift 2 ;;
    --paths)
      [[ -n "${2:-}" ]] || { echo "assert-no-duplicate-dispatch.sh: --paths needs a value" >&2; exit 64; }
      [[ "$2" != *$'\n'* && "$2" != *$'\r'* ]] || {
        echo "assert-no-duplicate-dispatch.sh: --paths contains a newline or carriage return; separate entries with commas" >&2
        exit 64
      }
      rest="$2,"
      while [[ -n "${rest}" ]]; do
        entry="${rest%%,*}"
        rest="${rest#*,}"
        entry="${entry#"${entry%%[![:space:]]*}"}"
        entry="${entry%"${entry##*[![:space:]]}"}"
        [[ -n "${entry}" ]] || {
          echo "assert-no-duplicate-dispatch.sh: --paths has an empty entry in '${2}'" >&2
          exit 64
        }
        entry_form "${entry}" >/dev/null || {
          echo "assert-no-duplicate-dispatch.sh: --paths entry '${entry}' is not path, path:L<start>-L<end> or path#symbol" >&2
          exit 64
        }
        paths+=("${entry}")
      done
      shift 2
      ;;
    --closed-days) closed_days="${2:-}"; shift 2 ;;
    --repo) repo="${2:-}"; shift 2 ;;
    --base-ref)
      [[ -n "${2:-}" ]] || { echo "assert-no-duplicate-dispatch.sh: --base-ref needs a value" >&2; exit 64; }
      base_ref="$2"; shift 2
      ;;
    *) echo "assert-no-duplicate-dispatch.sh: unexpected argument '$1'" >&2; exit 64 ;;
  esac
done

if [[ -z "${issue}" ]]; then
  echo "usage: assert-no-duplicate-dispatch.sh --issue <n> [--paths a,b] [--closed-days N]" >&2
  exit 64
fi
[[ "${issue}" =~ ^[0-9]+$ ]] || { echo "assert-no-duplicate-dispatch.sh: --issue must be a number" >&2; exit 64; }
[[ "${closed_days}" =~ ^[0-9]+$ ]] || { echo "assert-no-duplicate-dispatch.sh: --closed-days must be a number" >&2; exit 64; }

# A failed query must not read as "nothing found". Every gh call below
# either produces data or exits 69. A run issues one call per candidate pull
# request, a few hundred in a busy fortnight, so a single transient failure
# is retried before the run gives up.
ask_attempts="${ASK_ATTEMPTS:-3}"
ask_backoff_s="${ASK_BACKOFF_S:-5}"
ask() {
  local out err attempt=1
  err="$(mktemp)"
  while ! out="$("$@" 2>"${err}")"; do
    # A missing resource fails the same way every time; only a failure that
    # can pass on a second try is worth one.
    if ((attempt >= ask_attempts)) || grep -q 'HTTP 404\|Could not resolve to' "${err}"; then
      echo "assert-no-duplicate-dispatch.sh: '$*' failed ${attempt} times; refusing to report a clean answer" >&2
      sed -n '1,3p' "${err}" >&2
      rm -f "${err}"
      exit 69
    fi
    attempt=$((attempt + 1))
    sleep "$((ask_backoff_s * (attempt - 1)))"
  done
  rm -f "${err}"
  printf '%s' "${out}"
}

if [[ -z "${repo}" ]]; then
  repo="$(ask gh repo view --json nameWithOwner --jq .nameWithOwner)"
  [[ -n "${repo}" ]] || { echo "assert-no-duplicate-dispatch.sh: could not resolve the repository" >&2; exit 69; }
fi

issue_json="$(ask gh issue view "${issue}" --repo "${repo}" --json number,state,title,stateReason)"
issue_state="$(jq -r '.state' <<<"${issue_json}")"
issue_title="$(jq -r '.title' <<<"${issue_json}")"

if [[ "${issue_state}" == "CLOSED" ]]; then
  echo "SKIP  issue #${issue} is already CLOSED (${issue_title})"
  exit 65
fi

cutoff_epoch=$(( $(date +%s) - closed_days * 86400 ))

# Narrow lists rather than one of every pull request: the open ones,
# whatever their age, and those closed or merged on each day since the cutoff
# day, one query per day so no single list comes near its limit (this
# repository closes a few dozen a day, a few hundred a fortnight). A list
# that comes back full may have cut off a candidate, and a candidate that was
# never listed reads as no overlap, so a full list is refused.
pr_list_limit="${PR_LIST_LIMIT:-500}"
pr_fields="number,title,state,isDraft,body,headRefName,baseRefName,headRefOid,updatedAt,mergedAt,closedAt"
# Called inside a command substitution, where errexit does not apply, so each
# failure exits explicitly; falling through here would read as an empty list.
list_prs() {
  local listed count
  listed="$(ask gh pr list --repo "${repo}" --limit "${pr_list_limit}" --json "${pr_fields}" "$@")" || exit 69
  count="$(jq 'length' <<<"${listed}")" || exit 69
  [[ "${count}" =~ ^[0-9]+$ ]] || {
    echo "assert-no-duplicate-dispatch.sh: a pull request list length did not parse: ${count}" >&2
    exit 69
  }
  if ((count >= pr_list_limit)); then
    echo "assert-no-duplicate-dispatch.sh: a pull request list returned its limit of ${pr_list_limit}; it may be truncated, refusing to report a clean answer" >&2
    exit 69
  fi
  printf '%s' "${listed}"
}
open_json="$(list_prs --state open)" || exit 69
closed_json=""
for ((back = closed_days; back >= 0; back--)); do
  day_epoch=$(( $(date +%s) - back * 86400 ))
  day="$(date -u -r "${day_epoch}" +%Y-%m-%d 2>/dev/null \
    || date -u -d "@${day_epoch}" +%Y-%m-%d)"
  closed_json+="$(list_prs --state all --search "closed:${day}")" || exit 69
done
prs_json="$(jq -s 'add | unique_by(.number)' <<<"${open_json}${closed_json}")" || exit 69

# Candidates: every open pull request, plus anything closed or merged inside
# the window. A merged pull request from last week is exactly the thing that
# already did the work.
candidates="$(jq -r --argjson cutoff "${cutoff_epoch}" '
  [ .[] | select(
      .state == "OPEN"
      or ((.mergedAt // .closedAt // "") != ""
          and ((.mergedAt // .closedAt) | fromdateiso8601) >= $cutoff)
    ) ]
  | .[] | "\(.number)\t\(if .state == "OPEN" and .isDraft then "DRAFT" else .state end)\t\(if (.baseRefName // "") == "" then "-" else .baseRefName end)\t\(if (.headRefOid // "") == "" then "-" else .headRefOid end)\t\(.title)"' <<<"${prs_json}")"

# A pull request that CLOSES the issue, not one that merely cites it.
#
# This repository's commit convention makes the distinction load-bearing:
# `Fixes: #N` resolves the issue, `Refs: #N` says related and explicitly does
# not close it. Treating both as "already addressed" refuses genuine first
# dispatches -- measured on issue #1790, a coordination issue that two pull
# requests cite with `Refs:` and neither resolves; the guard refused it and
# pushed the operator toward DISPATCH_SKIP_DUPLICATE_CHECK=1, which trains
# exactly the reflex that flag exists to avoid. A guard worked around by
# habit has stopped being a guard.
#
# The keyword list is GitHub's own closing set, so what refuses here is what
# GitHub would actually close on merge. Two details carry that parity, and the
# first version of this got both wrong:
#
# A boundary on BOTH sides. With only a trailing one, any word ENDING in a
# keyword matched: `hotfix #42`, `bugfix #42`, `suffix: #42`, `prefix #42`,
# `affixes #42` and `disclosed #42` all refused a genuine first dispatch of
# #42, which is the same false-positive class this function was written to
# remove and which GitHub itself does not have (`hotfix #42` closes nothing).
#
# No -ing forms. GitHub closes on close/closes/closed, fix/fixes/fixed,
# resolve/resolves/resolved and nothing else; `fixing #42` does not close #42,
# so it must not refuse here either.
# Case is not part of the keyword. GitHub closes on `FIXES #42` and `fixes
# #42` alike, so matching only a capitalised first letter left an all-caps
# body reading as clean and allowed a second dispatch of a resolved ticket --
# the false-clean direction this file's header calls the only costly answer.
# The `; "i"` on each test below carries that; the pattern itself stays
# lower-case.
closing_kw='(^|[^0-9a-z])(clos(e|es|ed)|fix(es|ed)?|resolv(e|es|ed))'
# A colon absorbs the space: GitHub closes on `Fixes:#42` as well as
# `Fixes: #42` and `Fixes #42`. Requiring whitespace unconditionally missed
# the first. Something must still separate keyword from ref, so `Fixes42`
# does not match.
#
# `fixes#42`, with no separator at all, is NOT covered and is left that way
# deliberately: whether GitHub closes on it was not verified here, and
# guessing in the refusing direction blocks real work on a spelling nobody
# in this repository writes. If it turns out to close, `[[:space:]]*` on the
# second alternative covers it.
kw_sep='(:[[:space:]]*|[[:space:]]+)'
addressing="$(jq -r --arg needle "#${issue}" --arg kw "${closing_kw}" \
  --arg sep "${kw_sep}" --argjson cutoff "${cutoff_epoch}" '
  [ .[] | select(
      .state == "OPEN"
      or ((.mergedAt // .closedAt // "") != ""
          and ((.mergedAt // .closedAt) | fromdateiso8601) >= $cutoff)
    )
    | select(((.body // "") + " " + (.title // ""))
        | test($kw + $sep + $needle + "([^0-9]|$)"; "i"))
  ] | .[] | "\(.number)\t\(.state)\t\(.title)"' <<<"${prs_json}")"

# Cited but not closed. Worth knowing, never a refusal.
mentioning="$(jq -r --arg needle "#${issue}" --arg kw "${closing_kw}" \
  --arg sep "${kw_sep}" --argjson cutoff "${cutoff_epoch}" '
  [ .[] | select(
      .state == "OPEN"
      or ((.mergedAt // .closedAt // "") != ""
          and ((.mergedAt // .closedAt) | fromdateiso8601) >= $cutoff)
    )
    | select(((.body // "") + " " + (.title // ""))
        | test("(^|[^0-9A-Za-z])" + $needle + "([^0-9]|$)"))
    | select((((.body // "") + " " + (.title // ""))
        | test($kw + $sep + $needle + "([^0-9]|$)"; "i")) | not)
  ] | .[] | "\(.number)\t\(.state)\t\(.title)"' <<<"${prs_json}")"

if [[ -n "${addressing}" ]]; then
  echo "SKIP  issue #${issue} is already addressed:"
  while IFS=$'\t' read -r num state title; do
    [[ -z "${num}" ]] && continue
    echo "      #${num} [${state}] ${title}"
  done <<<"${addressing}"
  echo "      Do not dispatch a second task for it. Land, review or close that pull request instead."
  exit 65
fi

if [[ -n "${mentioning}" ]]; then
  echo "NOTE  issue #${issue} is cited by pull request(s) that do not close it:"
  while IFS=$'\t' read -r num state title; do
    [[ -z "${num}" ]] && continue
    echo "      #${num} [${state}] ${title}"
  done <<<"${mentioning}"
  echo "      Not a blocker. Read them before dispatching so the work is not redone."
fi

if ((${#paths[@]} == 0)); then
  echo "OK    no pull request closes issue #${issue}; no paths given, so no overlap check ran"
  exit 0
fi

# The base lines a patch changes: each removed line, and both neighbours of
# a pure insertion. Exits 3 on a non-empty patch with no hunk header.
# shellcheck disable=SC2016 # an awk program; its $ fields are awk's, not the shell's
changed_lines_awk='
  /^@@ -[0-9]/ {
    h = $2; sub(/^-/, "", h); n = split(h, p, ",")
    a = p[1] + 0; b = (n > 1) ? p[2] + 0 : 1
    base = (b == 0) ? a + 1 : a; rep = 0; seen = 1; next
  }
  !seen { next }
  /^-/ { print base; base++; rep = 1; next }
  /^\+/ { if (!rep) { print base - 1; print base }; next }
  /^\\/ { next }
  { base++; rep = 0 }
  END { if (!seen) exit 3 }'

# Exits 1 unless every context and removed line of the patch (read second)
# equals the base file's line (read first, named by bf) at the base line
# number its hunk header gives it, and 2 when a hunk has no such line.
# shellcheck disable=SC2016 # an awk program; its $ fields are awk's, not the shell's
hunk_check_awk='
  FILENAME == bf { line[FNR] = $0; next }
  /^@@ -[0-9]/ {
    if (seen && !compared) { bare = 1; exit }
    h = $2; sub(/^-/, "", h); split(h, p, ","); base = p[1] + 0; seen = 1; compared = 0; next
  }
  !seen || /^\+/ || /^\\/ { next }
  {
    if (!(base in line) || (line[base] "") != (substr($0, 2) "")) { bad = 1; exit }
    base++; compared = 1
  }
  END {
    if (bad) exit 1
    if (bare || (seen && !compared)) exit 2
  }'

# Opens or closes a fenced code block on a fence line. A fence closes only on
# the same character at least as long as the one that opened it.
# shellcheck disable=SC2016 # an awk function; its backticks are fence characters
fence_awk_fn='
  function fence(s,   c, n) {
    sub(/^ ? ? ?/, "", s); c = substr(s, 1, 1)
    if (c != "`" && c != "~") return 0
    n = 0; while (substr(s, n + 1, 1) == c) n++
    if (n < 3) return 0
    if (fch == "") { fch = c; flen = n; return 1 }
    if (c == fch && n >= flen) { fch = ""; return 1 }
    return 0
  }'

# lo:hi for every definition of `sym` in the file; see the header.
# shellcheck disable=SC2016 # an awk program; its $ anchors are awk's, not the shell's
symbol_awk='
  function anchor(s) {
    if (rust && s ~ ("^[ \t]*(pub(\\([^)]*\\))?[ \t]+)?(async[ \t]+|const[ \t]+|unsafe[ \t]+|default[ \t]+|extern[ \t]+(\"[^\"]*\"[ \t]+)?)*(fn|struct|enum|trait|mod|const|static|type|union)[ \t]+(mut[ \t]+)?" sym "([^A-Za-z0-9_]|$)")) return 1
    if (rust && s ~ /^[ \t]*(unsafe[ \t]+)?impl([ \t<]|$)/ && s ~ ("(^|[^A-Za-z0-9_])" sym "([^A-Za-z0-9_]|$)")) return 1
    if (shell && s ~ ("^[ \t]*(function[ \t]+)?" sym "[ \t]*\\(\\)")) return 1
    if (shell && s ~ ("^[ \t]*function[ \t]+" sym "([ \t{(]|$)")) return 1
    return 0
  }
  function top(s) {
    if (rust && s ~ /^(pub(\([^)]*\))?[ \t]+)?(async[ \t]+|const[ \t]+|unsafe[ \t]+|default[ \t]+|extern[ \t]+("[^"]*"[ \t]+)?)*(fn|struct|enum|trait|mod|const|static|type|union|impl)[ \t<]/) return 1
    if (shell && s ~ /^(function[ \t]+[A-Za-z0-9_-]+|[A-Za-z0-9_-]+[ \t]*\(\))/) return 1
    return 0
  }
  { line[NR] = $0 }
  END {
    for (i = 1; i <= NR; i++) {
      if (!anchor(line[i])) continue
      depth = 0; brackets = 0; opened = 0; end = 0
      for (j = i; j <= NR && !end; j++) {
        if (j > i && top(line[j])) { end = j - 1; break }
        n = length(line[j])
        for (k = 1; k <= n; k++) {
          c = substr(line[j], k, 1)
          if (c == "{") { depth++; opened = 1 }
          else if (c == "}") { depth--; if (opened && depth <= 0) { end = j; break } }
          else if (c == "(" || c == "[") brackets++
          else if (c == ")" || c == "]") brackets--
          else if (c == ";" && !opened && depth <= 0 && brackets <= 0) { end = j; break }
        }
      }
      if (!end) end = NR
      print i ":" end
    }
  }'

# lo:hi for every ATX heading whose GitHub anchor is `sym`, through its
# nested subsections.
# shellcheck disable=SC2016 # an awk program; its $ fields are awk's, not the shell's
heading_awk="${fence_awk_fn}"'
  function slug(s) { s = tolower(s); gsub(/[^a-z0-9 _-]/, "", s); gsub(/ /, "-", s); return s }
  fence($0) { next }
  fch == "" && /^(#|##|###|####|#####|######)([ \t]|$)/ {
    h++; at[h] = NR; t = $0; match(t, /^#+/); level[h] = RLENGTH
    sub(/^#+[ \t]*/, "", t); sub(/[ \t]*#*[ \t]*$/, "", t); name[h] = slug(t)
  }
  END {
    for (i = 1; i <= h; i++) {
      if (name[i] != slug(sym)) continue
      end = NR
      for (j = i + 1; j <= h; j++) if (level[j] <= level[i]) { end = at[j] - 1; break }
      print at[i] ":" end
    }
  }'

# The first changed line in `lines` whose section is one of the sections
# `ranges` covers. A line before the first heading is in section 0.
# shellcheck disable=SC2016 # an awk program; its $ fields are awk's, not the shell's
section_hit_awk="${fence_awk_fn}"'
  { fence($0) }
  fch == "" &&/^(#|##|###|####|#####|######)([ \t]|$)/ { cur = NR }
  { sec[NR] = cur }
  END {
    sec[0] = 0; sec[NR + 1] = sec[NR]
    n = split(ranges, rr, " ")
    for (i = 1; i <= n; i++) {
      split(rr[i], b, ":"); hi = (b[2] + 0 > NR + 1) ? NR + 1 : b[2] + 0
      for (l = b[1] + 0; l <= hi; l++) want[sec[l]] = 1
    }
    m = split(lines, ll, " ")
    for (i = 1; i <= m; i++) {
      l = ll[i] + 0; if (l < 0) l = 0; if (l > NR + 1) l = NR + 1
      if (sec[l] in want) { print ll[i]; exit }
    }
  }'

# Resolve every entry once against the base. e_mode is whole (any change to
# the file), lines (a changed line inside e_ranges) or sections (a changed
# line in a section e_ranges covers, read from e_base).
e_path=(); e_label=(); e_mode=(); e_ranges=(); e_base=(); e_blob=()
needs_base=0
for entry in "${paths[@]}"; do
  [[ "$(entry_form "${entry}")" == bare ]] || needs_base=1
done
scratch=""
if ((needs_base == 1)); then
  git rev-parse --verify --quiet "${base_ref}^{commit}" >/dev/null || {
    echo "assert-no-duplicate-dispatch.sh: base ref '${base_ref}' does not resolve; refusing to report a clean answer" >&2
    exit 69
  }
  scratch="$(mktemp -d)"
  trap 'rm -rf "${scratch}"' EXIT
fi
for entry in "${paths[@]}"; do
  form="$(entry_form "${entry}")"
  p="${entry%%[:#]*}"
  mode=whole; ranges=""; bfile=""
  docs=0
  [[ "${p}" == docs/*.md ]] && docs=1
  if [[ "${form}" != bare ]]; then
    bfile="${scratch}/base-${#e_path[@]}"
    git show "${base_ref}:${p}" >"${bfile}" 2>/dev/null || bfile=""
  fi
  case "${form}" in
    range)
      [[ "${entry}" =~ :L([0-9]+)-L([0-9]+)$ ]]
      ranges="${BASH_REMATCH[1]}:${BASH_REMATCH[2]}"
      if [[ -z "${bfile}" ]]; then
        echo "NOTE  ${entry}: ${p} is not on ${base_ref}; compared as the whole file"
      else
        last="$(awk 'END { print NR }' "${bfile}")"
        if ((BASH_REMATCH[1] > last)); then
          echo "NOTE  ${entry}: starts past the last line (${last}) of ${p} on ${base_ref}; compared as the whole file"
        elif ((docs == 1)); then
          mode=sections
        else
          mode=lines
        fi
      fi
      ;;
    symbol)
      sym="${entry#*#}"
      rust=0; shell=0
      if ((docs == 1)); then
        [[ -n "${bfile}" ]] && ranges="$(awk -v sym="${sym}" "${heading_awk}" "${bfile}")"
        mode=sections
      elif [[ "${p}" == *.rs ]]; then
        rust=1
      elif [[ "${p}" == *.sh || "${p}" == *.bash ]]; then
        shell=1
      fi
      if ((rust == 1 || shell == 1)); then
        [[ -n "${bfile}" ]] && ranges="$(awk -v sym="${sym}" -v rust="${rust}" -v shell="${shell}" "${symbol_awk}" "${bfile}")"
        mode=lines
      fi
      ranges="${ranges//$'\n'/ }"
      if ((docs == 0 && rust == 0 && shell == 0)); then
        echo "NOTE  ${entry}: symbol entries are not supported for this file type (only .rs, .sh, .bash and docs/*.md); compared as the whole file"
        mode=whole
      elif [[ -z "${ranges}" ]]; then
        echo "NOTE  ${entry}: '${sym}' not found in ${p} on ${base_ref}; compared as the whole file"
        mode=whole
      fi
      ;;
  esac
  blob=""
  if [[ "${mode}" != whole ]]; then
    blob="$(git rev-parse --verify --quiet "${base_ref}:${p}")" || {
      echo "assert-no-duplicate-dispatch.sh: could not read ${p} on ${base_ref}; refusing to report a clean answer" >&2
      exit 69
    }
  fi
  e_path+=("${p}"); e_label+=("${entry}"); e_mode+=("${mode}")
  e_ranges+=("${ranges}"); e_base+=("${bfile}"); e_blob+=("${blob}")
done

collision=0
overlap_report=""
context_report=""
files_cap=3000
while IFS=$'\t' read -r num state pr_base pr_head title; do
  [[ -z "${num}" ]] && continue
  # One line per file, a JSON array of its name, its patch (null when the
  # endpoint left the patch out), its name before a rename (null when the
  # pull request did not rename it) and its status.
  files="$(ask gh api "repos/${repo}/pulls/${num}/files" --paginate --jq '.[] | [.filename, .patch, .previous_filename, .status] | @json')" || exit 69
  shared=""
  nfiles="$(awk 'END { print NR }' <<<"${files}")"
  if ((nfiles >= files_cap)); then
    echo "NOTE  #${num} lists ${nfiles} files, the endpoint's limit; it may touch more, so every entry collides"
    for i in "${!e_path[@]}"; do shared+="${e_label[i]}, "; done
  fi
  mb_asked=0; mb=""; mb_why=""
  for i in "${!e_path[@]}"; do
    ((nfiles < files_cap)) || break
    recs="$(jq -c --arg p "${e_path[i]}" 'select(.[0] == $p or .[2] == $p)' <<<"${files}")" || exit 69
    [[ -n "${recs}" ]] || continue
    if [[ "${e_mode[i]}" == whole ]]; then
      shared+="${e_label[i]}, "
      continue
    fi
    why=""
    while IFS= read -r rec; do
      reason=""
      patch="$(jq -r 'if (.[1] // "") == "" then "-" else "+" + .[1] end' <<<"${rec}")" || exit 69
      status_="$(jq -r '.[3] // ""' <<<"${rec}")" || exit 69
      hunk_path="$(jq -r '.[2] // .[0]' <<<"${rec}")" || exit 69
      patch="${patch#+}"
      if [[ "${patch}" == "-" ]]; then
        reason="no patch in the API response"
      elif [[ "${status_}" == added || "${status_}" == removed ]] \
        || grep -Eq '^@@ -0,0 |^@@ -[0-9]+(,[0-9]+)? \+0,0 @@' <<<"${patch}"; then
        reason="the pull request adds or deletes this file"
      fi
      if [[ -z "${reason}" ]] && ((mb_asked == 0)); then
        mb_asked=1
        if [[ "${pr_base}" == - || "${pr_head}" == - ]]; then
          mb_why="the listing gives no base branch or head commit"
        else
          mb="$(ask gh api "repos/${repo}/compare/${pr_base}...${pr_head}" --jq '.merge_base_commit.sha // ""')" || exit 69
          if [[ ! "${mb}" =~ ^[0-9a-f]{7,64}$ ]]; then
            mb_why="the compare endpoint named no merge base"
          elif ! git cat-file -e "${mb}^{commit}" 2>/dev/null; then
            mb_why="merge base ${mb} is not available locally"
          fi
        fi
      fi
      if [[ -z "${reason}" && -n "${mb_why}" ]]; then
        reason="${mb_why}"
      elif [[ -z "${reason}" ]]; then
        mb_blob="$(git rev-parse --verify --quiet "${mb}:${hunk_path}")" || mb_blob=""
        if [[ -z "${mb_blob}" ]]; then
          reason="${hunk_path} is not on merge base ${mb}"
        elif [[ "${mb_blob}" != "${e_blob[i]}" ]]; then
          reason="changed on ${base_ref} since the pull request forked (merge base ${mb})"
        elif ! changed="$(printf '%s\n' "${patch}" | awk "${changed_lines_awk}")"; then
          reason="the patch did not parse"
        else
          hc=0
          printf '%s\n' "${patch}" | awk -v bf="${e_base[i]}" "${hunk_check_awk}" "${e_base[i]}" - || hc=$?
          if ((hc == 1)); then
            reason="a hunk does not match ${base_ref}"
          elif ((hc != 0)); then
            reason="a hunk has no context or removed line to check its numbering against"
          fi
        fi
      fi
      if [[ -n "${reason}" ]]; then
        echo "NOTE  #${num} ${e_path[i]}: ${reason}; ${e_label[i]} compared as the whole file"
        why="${reason}; compared as the whole file"
        break
      fi
      changed="${changed//$'\n'/ }"
      hit=""
      if [[ "${e_mode[i]}" == sections ]]; then
        hit="$(awk -v ranges="${e_ranges[i]}" -v lines="${changed}" "${section_hit_awk}" "${e_base[i]}")"
      else
        for l in ${changed}; do
          for r in ${e_ranges[i]}; do
            if ((l >= ${r%%:*} && l <= ${r##*:})); then hit="${l}"; break 2; fi
          done
        done
      fi
      if [[ -n "${hit}" ]]; then
        why="base line ${hit}"
        break
      fi
    done <<<"${recs}"
    [[ -n "${why}" ]] && shared+="${e_label[i]} (${why}), "
  done
  shared="${shared%, }"
  if [[ -n "${shared}" ]]; then
    # Only an OPEN, non-draft pull request is a collision: a draft is held
    # work nobody is landing (a held draft once blocked two epics' tickets for
    # a day), and a merged or closed one is already on main or abandoned, so
    # both are reported for context under their own heading, never as the
    # reason for the refusal.
    if [[ "${state}" == "OPEN" ]]; then
      overlap_report+="      #${num} [${state}] ${title}"$'\n'"        shared: ${shared% }"$'\n'
      collision=1
    else
      context_report+="      #${num} [${state}] ${title}"$'\n'"        shared: ${shared% }"$'\n'
    fi
  fi
done <<<"${candidates}"

if [[ -z "${overlap_report}" && -z "${context_report}" ]]; then
  echo "OK    issue #${issue}: no referencing pull request, no file overlap across ${#paths[@]} predicted path(s)"
  exit 0
fi

if ((collision == 1)); then
  echo "COLLIDE  an OPEN pull request is already touching these files:"
  printf '%s' "${overlap_report}"
  if [[ -n "${context_report}" ]]; then
    echo "         Also on these files, not a collision (merged, closed or draft):"
    printf '%s' "${context_report}"
  fi
  echo "         Dispatching over them produces two divergent rewrites of the same code."
  echo "         Wait for it to land, or fold the work into that pull request."
  exit 66
fi

echo "NOTE  merged, closed or draft pull requests touched these files; nothing open and ready:"
printf '%s' "${context_report}"
echo "      Not a blocker. Read them before dispatching so the work is not redone."
exit 0
