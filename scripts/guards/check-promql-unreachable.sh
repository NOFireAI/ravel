#!/usr/bin/env bash
# PromQL unreachable! guard (issue #1701).
#
# A parsed tenant query used to be able to reach `unreachable!()` arms in the
# PromQL evaluator: an untrusted-shaped AST hit a defensive fallback the
# evaluator's author believed the parser could never produce, and the
# process aborted instead of rejecting the query. That round converted every
# reachable arm to a typed `Error::Unsupported` rejection and left only the
# arms an exhaustive prior match already narrows out of reach.
#
# This guard keeps it that way: every `unreachable!` under
# crates/ravel-promql/src must carry, on its own line or anywhere in the
# contiguous comment block directly above it, a marker naming the arm that
# now rejects first and why this one still cannot be reached:
#
#   // unreachable-allow: <the arm that rejects first> -- <reason>
#
# The reason is required (text after the `--`, on the marker's own line or
# continuing on the comment lines below it up to the next non-comment line).
# A marker with no reason does not suppress. An `unreachable!` added later
# with no marker at all is exactly the case this guard exists to catch: it
# means either the arm is reachable and should return `Error::Unsupported`
# instead, or it is genuinely dead and needs the same one-sentence
# justification every existing arm already carries.
#
# Usage:
#   scripts/guards/check-promql-unreachable.sh [path ...]
#     default: crates/ravel-promql/src
#
# Exit 0 clean, 1 on an unmarked (or empty-reason) `unreachable!`, 2 when the
# scan finds zero `unreachable!` occurrences or the source directory is
# missing (a rename or move must not silently turn this into a no-op), 64 on
# bad usage. Findings print as `file:line: bare-unreachable: <explanation>`.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,29p' "$0"
      exit 0
      ;;
    -*)
      echo "check-promql-unreachable.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=(crates/ravel-promql/src)
fi

for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-promql-unreachable.sh: no such directory: ${root}" >&2
    exit 2
  fi
done

sources=()
while IFS= read -r -d '' file; do
  sources+=("${file}")
done < <(find "${roots[@]}" -type f -name '*.rs' -not -path '*/target/*' -print0 | sort -z)

if [[ ${#sources[@]} -eq 0 ]]; then
  echo "check-promql-unreachable.sh: no Rust sources under ${roots[*]}" >&2
  exit 2
fi

# `unreachable!` only counts as code, not as prose about it in a doc comment
# or a quoted string. Same line-stripping approach as
# check-guarded-sql-parse.sh: track string/raw-string/block-comment state
# across lines and split each line into a code part and a comment part.
awk_prog='
function charlit_len(s, i,   c2, c, j) {
  c2 = substr(s, i + 1, 1)
  if (c2 == "\\") {
    j = i + 2
    c = substr(s, j, 1)
    if (c == "x") j = j + 3
    else if (c == "u") {
      j = j + 1
      while (j <= length(s) && substr(s, j, 1) != "}") j++
      j++
    } else j = j + 1
    if (substr(s, j, 1) == SQ) return j - i + 1
    return 0
  }
  if (substr(s, i + 2, 1) == SQ) return 3
  return 0
}
function strip_line(s,   out, i, n, c, j, k, hashes, hs, cl) {
  out = ""; cmt = ""; n = length(s); i = 1
  while (i <= n) {
    c = substr(s, i, 1)
    if (bdepth > 0) {
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; i += 2; continue }
      cmt = cmt c; i++; continue
    }
    if (sstate == 1) {
      if (sesc) { sesc = 0; i++; continue }
      if (c == "\\") { sesc = 1; i++; continue }
      if (c == "\"") sstate = 0
      i++; continue
    }
    if (sstate == 2) {
      if (c == "\"") {
        hs = ""
        for (j = 0; j < shashes; j++) hs = hs "#"
        if (shashes == 0 || substr(s, i + 1, shashes) == hs) {
          sstate = 0; i += 1 + shashes; continue
        }
      }
      i++; continue
    }
    if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth = 1; i += 2; continue }
    if (c == "/" && substr(s, i + 1, 1) == "/") { cmt = cmt substr(s, i); break }
    if (c == SQ) {
      cl = charlit_len(s, i)
      if (cl > 0) { i += cl; continue }
      out = out c; i++; continue
    }
    if (c == "r" || (c == "b" && substr(s, i + 1, 1) == "r")) {
      j = i
      if (c == "b") j++
      k = j + 1
      hashes = 0
      while (substr(s, k, 1) == "#") { hashes++; k++ }
      if (substr(s, k, 1) == "\"") { sstate = 2; shashes = hashes; i = k + 1; continue }
    }
    if (c == "\"") { sstate = 1; i++; continue }
    out = out c; i++
  }
  return out
}
function mentions_unreachable(s) {
  return s ~ /(^|[^A-Za-z0-9_])unreachable!/
}
# The marker itself only counts from comment text, and takes the form
# `unreachable-allow: <arm> -- <reason>`. A reason is required: either text
# after the "--" on the same line, or (the common case, since the arm name
# runs the line long) the marker line ends right after "--" and the reason
# continues as plain comment text on the contiguous lines below it.
# Returns: 0 no marker, 1 marker with a reason already on this line, 2
# marker present but the reason (if any) is still to come on a later line.
function has_marker_with_reason(s,   i, rest, dashpos, reason) {
  i = index(s, MARKER)
  if (i == 0) return 0
  rest = substr(s, i + length(MARKER))
  dashpos = index(rest, "--")
  if (dashpos == 0) return 2
  reason = substr(rest, dashpos + 2)
  gsub(/[ \t]/, "", reason)
  if (length(reason) > 0) return 1
  return 2
}
BEGIN {
  SQ = sprintf("%c", 39)
  MARKER = "unreachable-allow:"
  findings = 0
  total = 0
}
FNR == 1 {
  sstate = 0; sesc = 0; shashes = 0; bdepth = 0
  block_marker = 0; block_has_reason = 0
}
{
  code = strip_line($0)
  is_comment_line = ($0 ~ /^[ \t]*\/\// || $0 ~ /^[ \t]*\/\*/ || $0 ~ /^[ \t]*\*/)

  if (mentions_unreachable(code)) {
    total++
    m = has_marker_with_reason(cmt)
    inline_ok = (m == 1)
    if (!inline_ok && !(block_marker && block_has_reason)) {
      printf "%s:%d: bare-unreachable: no `%s <arm> -- <reason>` marker on" \
             " this line or in the comment block above it; either this arm" \
             " is reachable by a parsed query and must return" \
             " Error::Unsupported instead, or it needs the same" \
             " justification every other unreachable! in this crate carries\n", \
             FILENAME, FNR, MARKER
      findings++
    }
  }

  if (is_comment_line) {
    m = has_marker_with_reason(cmt)
    if (m == 1) { block_marker = 1; block_has_reason = 1 }
    else if (m == 2 && !block_marker) { block_marker = 1; block_has_reason = 0 }
    else if (block_marker && !block_has_reason && cmt != "") {
      # a later comment line in the same block supplies the reason text
      trimmed = cmt
      gsub(/^[ \t]*\/\/+[ \t]*/, "", trimmed)
      gsub(/[ \t]/, "", trimmed)
      if (length(trimmed) > 0) block_has_reason = 1
    }
  } else {
    block_marker = 0
    block_has_reason = 0
  }
}
END {
  if (total == 0) {
    printf "check-promql-unreachable.sh: found zero unreachable! occurrences" \
           " under the scanned roots; expected at least the arms an" \
           " exhaustive prior match already narrows out of reach. A rename" \
           " or move must not silently empty this scan (issue #1701)\n" > "/dev/stderr"
    exit 2
  }
  if (findings > 0) exit 1
  exit 0
}
'

awk "${awk_prog}" "${sources[@]}"
rc=$?
if [[ ${rc} -eq 0 ]]; then
  echo "check-promql-unreachable.sh: every unreachable! is documented"
fi
exit ${rc}
