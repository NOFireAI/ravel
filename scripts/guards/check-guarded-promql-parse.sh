#!/usr/bin/env bash
# Guarded-PromQL-parse guard (issue #1817).
#
# Every parse of caller text must run the pre-parse structural-complexity
# guard first: `promql_parser`'s parser is recursive descent with no depth
# bound of its own, so an over-bound query aborts the process during parsing
# and takes every tenant on the node with it (issue #529).
# `ravel_promql::complexity_guard::parse_guarded` is the one function that runs
# the check and then parses. This guard refuses any other mention of a PromQL
# parser front end under the two roots below, so a new entry point that parses
# on its own fails the gate instead of resting on its author knowing the rule.
# The SQL side of the same rule became a check for the same reason (issue
# #1760): the convention held everywhere anyone looked, and one parse site had
# never had it at all.
#
# TWO ROOTS, because the PromQL funnel has a caller in another crate:
# crates/ravel-promql/src (where the funnel lives) and crates/ravel-query/src
# (where the federated-query path parses a selector). A scan of the first root
# alone passes every in-crate case and leaves the cross-crate entry point
# unguarded, which is exactly the shape this ticket exists for. Both roots are
# required: this guard refuses to run over one.
#
# TWO ANCHORS, so a rename cannot turn the scan into a no-op that passes
# everything. The first is `fn parse_guarded` in the funnel's own file, which
# must exist exactly once and must still reach a parser. The second is a
# mention of `parse_guarded` under the second root: if the cross-crate caller
# stops routing through the funnel, this guard says so rather than reporting
# the root clean. Neither is a general cross-crate checker, on purpose: two
# explicit roots are checkable by eye, and a checker that follows funnels
# across crate boundaries is a larger thing than the rule it would enforce.
#
# Refused, outside comments and string literals: a line mentioning
# `promql_parser` together with a parse entry point (`parse`, `parse_expr`,
# `lexer`, `lex`); a line of a `use` statement that imports one; a call to a
# name such a `use` brought into scope (including through an alias, and
# including `parser::parse` through a `self` import of the parser module).
# A `parse` with no `promql_parser` in sight (`str::parse`, a `Params::parse`
# constructor) is not a finding: reaching this parser means naming it or
# importing it.
#
# Usage:
#   scripts/guards/check-guarded-promql-parse.sh [first-root second-root ...]
#     default: crates/ravel-promql/src crates/ravel-query/src
#
# Exit 0 clean, 1 on findings, 2 when the scan would be a no-op (an anchor is
# gone, a default root is missing, or the roots hold no Rust sources), 64 on
# bad usage (an unknown option, a named root that is not there, fewer than two
# roots). Findings print as `file:line: bare-parse: explanation`.
#
# The difference from check-guarded-sql-parse.sh's codes is deliberate and it
# is one case: that script exits 64 for a scan with no sources, which is a
# usage answer to a "would this scan find anything at all" question. Here the
# empty scan is the failure mode the anchors exist for, so it exits 2 with
# them. A root named on the command line that does not exist is still 64: that
# is a caller's typo, not the tree moving under a default.
#
# Escape hatch, per finding, on the flagged line's own comment or anywhere in
# the contiguous comment block immediately above it:
#
#   // guarded-parse-allow: <reason>
#
# The reason is required: the marker without one does not suppress. Test code
# that needs the raw front end (asserting on the parser's own error text, say)
# is what it is for. Production code is not: route it through
# `ravel_promql::complexity_guard::parse_guarded`.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

# The one function allowed to reach a parser, and the file it lives in.
anchor_file="crates/ravel-promql/src/complexity_guard.rs"
anchor_fn="parse_guarded"

default_roots=(crates/ravel-promql/src crates/ravel-query/src)

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,66p' "$0"
      exit 0
      ;;
    -*)
      echo "check-guarded-promql-parse.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done

roots_from_cli=1
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=("${default_roots[@]}")
  roots_from_cli=0
fi

if [[ ${#roots[@]} -lt 2 ]]; then
  echo "check-guarded-promql-parse.sh: needs at least two roots; the" \
    "cross-crate entry point is the reason this guard exists, and a scan of" \
    "the funnel's own crate alone passes it silently" >&2
  exit 64
fi

# A missing root is a usage error when the caller named it and an anchor
# failure when it is a default: the default moving means the crate moved, and
# a scan that quietly covers one less root than it claims is the no-op this
# guard refuses to be.
missing_rc=2
if [[ ${roots_from_cli} -eq 1 ]]; then
  missing_rc=64
fi
for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-guarded-promql-parse.sh: no such directory: ${root}" >&2
    exit "${missing_rc}"
  fi
done

second_root="${roots[1]}"

sources=()
while IFS= read -r -d '' file; do
  sources+=("${file}")
done < <(find "${roots[@]}" -type f -name '*.rs' -not -path '*/target/*' -print0 | sort -z)

if [[ ${#sources[@]} -eq 0 ]]; then
  echo "check-guarded-promql-parse.sh: no Rust sources under ${roots[*]};" \
    "a scan over zero files reports clean, which is the state this guard" \
    "refuses rather than reports" >&2
  exit 2
fi

# A mention of a parser front end only counts as code. These sources carry
# hundreds of lines of prose about the parser in doc comments, and an
# assertion message can quote it too, so the scan reads the code skeleton of
# each line and the comment text separately: the first is where a finding
# lives, the second is where the allow marker must be.
#
# The stripper is check-guarded-sql-parse.sh's, including its state carried
# across lines (a Rust string literal spans lines, so a per-line reset scans
# the second line of a multi-line message as code) and its char-literal
# handling (a lone `'` in `&'a str` would otherwise open a string that
# swallows the rest of the file, which is the direction that makes the guard
# blind).
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
# A parse entry point named as its own token. Deliberately broad on its own:
# it is only ever read together with a promql_parser context below.
function entry_token(s) {
  return s ~ /(^|[^A-Za-z0-9_])(parse|parse_expr|lexer|lex)([^A-Za-z0-9_]|$)/
}
# A free call of `name`, not a method call (`raw.parse()`) and not a path
# (`Params::parse`, which the qualified rules handle).
function free_call(s, name) {
  return s ~ ("(^|[^A-Za-z0-9_.:])" name "[ \t]*\\(")
}
# `name::parse`, for a module brought into scope by a `use`.
function mod_entry(s, name) {
  return s ~ ("(^|[^A-Za-z0-9_])" name "::(parse|parse_expr|lexer|lex)([^A-Za-z0-9_]|$)")
}
function alias_after(buf, what,   m, parts, n) {
  if (!match(buf, what "[ \t]+as[ \t]+[A-Za-z_][A-Za-z0-9_]*")) return ""
  m = substr(buf, RSTART, RLENGTH)
  n = split(m, parts, /[ \t]+/)
  return parts[n]
}
# What a completed `use` statement brings into scope, when it is a use of
# promql_parser at all. Names land in fn_scope (called directly) or mod_scope
# (used as a path prefix).
function record_use(buf,   a) {
  if (buf !~ /promql_parser/) return
  if (buf ~ /(^|[^A-Za-z0-9_])(parse|parse_expr)([^A-Za-z0-9_]|$)/) fn_scope["parse"] = 1
  if (buf ~ /(^|[^A-Za-z0-9_])lexer([^A-Za-z0-9_]|$)/) fn_scope["lexer"] = 1
  a = alias_after(buf, "(parse|parse_expr|lexer)")
  if (a != "") fn_scope[a] = 1
  if (buf ~ /parser[ \t]*::[ \t]*\{[^}]*self/ ||
      buf ~ /::[ \t]*parser[ \t]*[;,}]/) mod_scope["parser"] = 1
  a = alias_after(buf, "parser")
  if (a != "") mod_scope[a] = 1
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
  second_root_funnel = 0
}
FNR == 1 {
  sstate = 0; sesc = 0; shashes = 0; bdepth = 0; in_anchor = 0; block_marker = 0
  use_open = 0; use_buf = ""
  split("", fn_scope); split("", mod_scope)
  in_second_root = (index(FILENAME, SECOND_ROOT) == 1)
}
{
  code = strip_line($0)

  if (FILENAME == ANCHOR_FILE && code ~ ("(^|[^A-Za-z0-9_])fn[ \t]+" ANCHOR_FN "[ \t]*\\(")) {
    anchor_defs++
    in_anchor = 1
  }
  if (in_second_root && code ~ ("(^|[^A-Za-z0-9_])" ANCHOR_FN "([^A-Za-z0-9_]|$)")) {
    second_root_funnel++
  }

  if (!use_open && code ~ /(^|[^A-Za-z0-9_])use[ \t]/) { use_open = 1; use_buf = "" }
  if (use_open) use_buf = use_buf " " code

  found = 0
  if (entry_token(code)) {
    if (code ~ /promql_parser/) found = 1
    else if (use_open && use_buf ~ /promql_parser/) found = 1
    else {
      for (n in mod_scope) if (mod_entry(code, n)) found = 1
      for (n in fn_scope) if (free_call(code, n)) found = 1
    }
  }

  if (found) {
    if (in_anchor) {
      anchor_mentions++
    } else if (!has_marker(cmt) && !block_marker) {
      printf "%s:%d: bare-parse: reaches a PromQL parser front end outside %s::%s;" \
             " parse through it so the complexity guard cannot be skipped" \
             " (issue #1817), or mark the line with `// %s <reason>`\n", \
             FILENAME, FNR, ANCHOR_FILE, ANCHOR_FN, MARKER
      findings++
    }
  }

  if (use_open && code ~ /;/) { record_use(use_buf); use_open = 0 }

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
           " guarded parse again (issue #1817)\n", \
           ANCHOR_FILE, ANCHOR_FN, anchor_defs > "/dev/stderr"
    exit 2
  }
  if (anchor_mentions == 0) {
    printf "%s: anchor: `fn %s` no longer reaches a PromQL parser, so the" \
           " identifiers this guard refuses are the wrong ones (issue #1817)\n", \
           ANCHOR_FILE, ANCHOR_FN > "/dev/stderr"
    exit 2
  }
  if (second_root_funnel == 0) {
    printf "%s: anchor: the second root routes nothing through %s." \
           " Either its PromQL entry point moved (re-point this guard) or it" \
           " has none left (drop the root). A root scanned for a rule it no" \
           " longer takes part in reports clean whatever it holds" \
           " (issue #1817)\n", \
           SECOND_ROOT, ANCHOR_FN > "/dev/stderr"
    exit 2
  }
  if (findings > 0) exit 1
  exit 0
}
'

awk -v ANCHOR_FILE="${anchor_file}" -v ANCHOR_FN="${anchor_fn}" \
  -v SECOND_ROOT="${second_root}" "${awk_prog}" "${sources[@]}"
rc=$?
if [[ ${rc} -eq 0 ]]; then
  echo "check-guarded-promql-parse.sh: every PromQL parse goes through ${anchor_fn}"
fi
exit ${rc}
