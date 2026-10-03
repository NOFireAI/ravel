#!/usr/bin/env bash
# Guarded-SQL-parse guard (issue #1760).
#
# Every parse of caller text in ravel-sql must run the pre-parse
# structural-complexity guard first: the parser's own recursion limit does not
# bound a flat operator chain, and the walks over the parsed tree recurse once
# per tree level, so an over-bound statement aborts the process and takes every
# tenant on the node with it. `complexity_guard::parse_guarded` is the one
# function that runs the check and then builds the parser. This guard refuses
# any other mention of a SQL parser front end under crates/ravel-sql/src/, so a
# new entry point that builds its own parser fails the gate instead of relying
# on its author knowing the rule. The convention form of this rule failed the
# first time it was tested: the audit path shipped a review round without the
# check, and the page plan had none at all.
#
# Refused identifiers, outside comments and string literals: DFParser,
# DFParserBuilder, and sqlparser's Parser (naming any of them is enough, since
# a name is how the parser is reached, whether through a `use` or a fully
# qualified path).
#
# Usage:
#   scripts/guards/check-guarded-sql-parse.sh [path ...]
#     default: crates/ravel-sql/src
#
# Exit 0 clean, 1 on findings, 2 when the anchor the guard rests on is gone
# (no `fn parse_guarded`, or one that no longer mentions a parser, which would
# leave this scan passing everything), 64 on bad usage. Findings print as
# `file:line: bare-parse: explanation`.
#
# Escape hatch, per finding, on the flagged line's own comment or anywhere in
# the contiguous comment block immediately above it:
#
#   // guarded-parse-allow: <reason>
#
# The reason is required: the marker without one does not suppress. Test code
# that parses a fixture through a raw front end is what it is for. Production
# code is not: route it through `complexity_guard::parse_guarded`.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

# The one function allowed to build a parser, and the file it lives in. The
# guard asserts both still exist: a rename that emptied this scan would
# otherwise leave a check that passes everything.
anchor_file="crates/ravel-sql/src/complexity_guard.rs"
anchor_fn="parse_guarded"

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,37p' "$0"
      exit 0
      ;;
    -*)
      echo "check-guarded-sql-parse.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=(crates/ravel-sql/src)
fi

for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-guarded-sql-parse.sh: no such directory: ${root}" >&2
    exit 64
  fi
done

sources=()
while IFS= read -r -d '' file; do
  sources+=("${file}")
done < <(find "${roots[@]}" -type f -name '*.rs' -not -path '*/target/*' -print0 | sort -z)

if [[ ${#sources[@]} -eq 0 ]]; then
  echo "check-guarded-sql-parse.sh: no Rust sources under ${roots[*]}" >&2
  exit 64
fi

# A mention of a parser front end only counts as code. These sources carry
# hundreds of lines of prose about the parser in doc comments, and an
# assertion message can quote it too, so the scan reads the code skeleton of
# each line and the comment text separately: the first is where a finding
# lives, the second is where the allow marker must be.
#
# The stripper follows the one in check-test-hygiene.sh, including its state
# carried across lines (a Rust string literal spans lines, so a per-line reset
# scans the second line of a multi-line message as code) and its char-literal
# handling (a lone `'` in `&'a str` would otherwise open a string that swallows
# the rest of the file, which is the direction that makes the guard blind).
awk_prog='
function charlit_len(s, i,   c2, c, j) {
  # s[i] is a single quote. Return the length of a char literal starting there,
  # or 0 when the quote opens a lifetime/label instead.
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
    if (bdepth > 0) {                           # inside /* ... */, nested
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; i += 2; continue }
      cmt = cmt c; i++; continue
    }
    if (sstate == 1) {                          # inside "..."
      if (sesc) { sesc = 0; i++; continue }
      if (c == "\\") { sesc = 1; i++; continue }
      if (c == "\"") sstate = 0
      i++; continue
    }
    if (sstate == 2) {                          # inside a raw string
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
      out = out c; i++; continue              # a lifetime tick: keep it
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
function mentions_parser(s) {
  return s ~ /(^|[^A-Za-z0-9_])(DFParser|DFParserBuilder|Parser)([^A-Za-z0-9_]|$)/
}
# The marker counts only from comment text, and only with a reason after it.
function has_marker(s,   i, rest) {
  i = index(s, MARKER)
  if (i == 0) return 0
  rest = substr(s, i + length(MARKER))
  gsub(/[ \t]/, "", rest)
  return length(rest) > 0
}
BEGIN {
  SQ = sprintf("%c", 39)          # a single quote, unwriteable in this quoting
  MARKER = "guarded-parse-allow:"
  findings = 0
  anchor_defs = 0
  anchor_mentions = 0
}
FNR == 1 { sstate = 0; sesc = 0; shashes = 0; bdepth = 0; in_anchor = 0; block_marker = 0 }
{
  code = strip_line($0)
  is_anchor_file = (FILENAME == ANCHOR_FILE)

  if (is_anchor_file && code ~ ("(^|[^A-Za-z0-9_])fn[ \t]+" ANCHOR_FN "[ \t]*\\(")) {
    anchor_defs++
    in_anchor = 1
  }

  if (mentions_parser(code)) {
    if (in_anchor) {
      anchor_mentions++
    } else if (!has_marker(cmt) && !block_marker) {
      printf "%s:%d: bare-parse: reaches a SQL parser front end outside %s::%s;" \
             " parse through it so the complexity guard cannot be skipped" \
             " (issue #1760), or mark the line with `// %s <reason>`\n", \
             FILENAME, FNR, ANCHOR_FILE, ANCHOR_FN, MARKER
      findings++
    }
  }

  # A comment block is contiguous: any non-comment line ends it, so a marker
  # cannot carry past the code it was written for.
  if ($0 ~ /^[ \t]*\/\//) {
    if (has_marker(cmt)) block_marker = 1
  } else {
    block_marker = 0
  }

  # The anchor function ends at a closing brace in column 0.
  if (in_anchor && $0 ~ /^}/) in_anchor = 0
}
END {
  if (anchor_defs != 1) {
    printf "%s: anchor: expected exactly one `fn %s`, found %d." \
           " Without it this guard passes everything: point it at the" \
           " guarded parse again (issue #1760)\n", \
           ANCHOR_FILE, ANCHOR_FN, anchor_defs > "/dev/stderr"
    exit 2
  }
  if (anchor_mentions == 0) {
    printf "%s: anchor: `fn %s` no longer builds a parser, so the identifiers" \
           " this guard refuses are the wrong ones (issue #1760)\n", \
           ANCHOR_FILE, ANCHOR_FN > "/dev/stderr"
    exit 2
  }
  if (findings > 0) exit 1
  exit 0
}
'

awk -v ANCHOR_FILE="${anchor_file}" -v ANCHOR_FN="${anchor_fn}" "${awk_prog}" "${sources[@]}"
rc=$?
if [[ ${rc} -eq 0 ]]; then
  echo "check-guarded-sql-parse.sh: every SQL parse goes through ${anchor_fn}"
fi
exit ${rc}
