#!/usr/bin/env bash
# MCP error-display guard (issue #4292).
#
# No failure the MCP layer hands a caller may carry an internal error's
# Display text: it can name an object key, a corrupt value or another detail
# the operator's log is for. The fixed sites return a fixed message and log
# the cause at warn with the tenant hash (services/ravel-server/src/mcp/
# envelope.rs, `log_redacted` and `redacted_internal`). This guard refuses a
# new site that formats text into a caller-facing failure, so the rule does
# not rest on its author knowing it.
#
# What it refuses, outside comments and outside #[cfg(test)] test modules:
# the argument text of a call to `d4::internal(`, `d4::failure(` (also
# spelled `envelope::`), any `McpError::<constructor>(`, and a `json!(` that
# has a "reason" key, when that text holds `format!`, `format_args!`,
# `to_string` (except on a string literal, `"fixed".to_string()`), or an
# interpolated `{error}`, `{err}` or `{e}`. The argument text runs from the
# call's opening parenthesis to its matching close, across lines; parentheses
# inside string and char literals do not count.
#
# What it does not see: text formatted into a variable on an earlier line and
# passed in by name, and constructors it does not list (`d4::invalid_argument`
# carries caller-input errors by design, and a `Failure { .. }` literal).
#
# Test code is out of scope: a line holding only `#[cfg(test)]`, followed
# (past blank, comment and attribute lines) by `mod tests`, optionally with a
# visibility, skips the rest of that file. A `#[cfg(test)]` on anything else
# skips nothing.
#
# Usage:
#   scripts/guards/check-mcp-error-display.sh [--list-allowed] [path ...]
#     default: services/ravel-server/src/mcp
#     --list-allowed: also print each call a marker suppressed, as
#     `file:line: allowed: ...`, for a class-closure audit.
#
# Exit 0 clean, 1 on findings, 2 when the scan cannot mean anything: the
# default directory is gone, it holds no Rust sources, or the sources hold
# none of the calls above (a move or a rename would otherwise turn this into
# a check that passes everything). 64 on bad usage, including a named path
# that does not exist. Findings print as `file:line: error-display: ...`.
#
# Escape hatch, per call, on the call's first line or anywhere in the
# contiguous comment block immediately above that line:
#
#   // mcp-error-display-allow: <reason>
#
# The reason is required: the marker without one does not suppress. It is for
# a site whose formatted text only echoes caller input or fixed values. A
# marker on an enclosing call covers the calls nested in its arguments.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

default_root="services/ravel-server/src/mcp"

roots=()
list_allowed=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,49p' "$0"
      exit 0
      ;;
    --list-allowed)
      list_allowed=1
      shift
      ;;
    -*)
      echo "check-mcp-error-display.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done
if [[ ${#roots[@]} -eq 0 ]]; then
  if [[ ! -d "${default_root}" ]]; then
    echo "check-mcp-error-display.sh: ${default_root} is gone; point this guard at the MCP sources again (issue #4292)" >&2
    exit 2
  fi
  roots=("${default_root}")
fi

for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-mcp-error-display.sh: no such directory: ${root}" >&2
    exit 64
  fi
done

sources=()
while IFS= read -r -d '' file; do
  sources+=("${file}")
done < <(find "${roots[@]}" -type f -name '*.rs' -not -path '*/target/*' -print0 | sort -z)

if [[ ${#sources[@]} -eq 0 ]]; then
  echo "check-mcp-error-display.sh: no Rust sources under ${roots[*]}; a scan of nothing passes everything (issue #4292)" >&2
  exit 2
fi

# The stripper turns each line into two strings of the same length, so a
# position in one is the same position in the other (LC_ALL=C makes awk count
# bytes). CODE blanks comments, string contents and char literals to spaces:
# calls and parentheses are found and counted there. FULL keeps string
# contents, with a quote inside a literal swapped for a control byte so a
# literal is one `"..."` run: the argument text is read there. Comment text goes
# to `cmt`, where the allow marker must be. String state carries across lines
# because a Rust string literal can span them.
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
function spaces(k,   r) {
  r = ""
  while (k-- > 0) r = r " "
  return r
}
function strip_line(s,   i, n, c, j, k, hashes, hs, cl) {
  CODE = ""; FULL = ""; cmt = ""; n = length(s); i = 1
  while (i <= n) {
    c = substr(s, i, 1)
    if (bdepth > 0) {
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; CODE = CODE "  "; FULL = FULL "  "; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; CODE = CODE "  "; FULL = FULL "  "; i += 2; continue }
      cmt = cmt c; CODE = CODE " "; FULL = FULL " "; i++; continue
    }
    if (sstate == 1) {
      if (sesc) { sesc = 0; CODE = CODE " "; FULL = FULL c; i++; continue }
      if (c == "\\") { sesc = 1; CODE = CODE " "; FULL = FULL c; i++; continue }
      if (c == "\"") { sstate = 0; CODE = CODE CQ; FULL = FULL CQ; i++; continue }
      CODE = CODE " "; FULL = FULL c; i++; continue
    }
    if (sstate == 2) {
      if (c == "\"") {
        hs = ""
        for (j = 0; j < shashes; j++) hs = hs "#"
        if (shashes == 0 || substr(s, i + 1, shashes) == hs) {
          sstate = 0
          CODE = CODE CQ hs; FULL = FULL CQ hs
          i += 1 + shashes; continue
        }
      }
      CODE = CODE " "; FULL = FULL c; i++; continue
    }
    if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth = 1; CODE = CODE "  "; FULL = FULL "  "; i += 2; continue }
    if (c == "/" && substr(s, i + 1, 1) == "/") {
      cmt = cmt substr(s, i)
      CODE = CODE spaces(n - i + 1); FULL = FULL spaces(n - i + 1)
      break
    }
    if (c == SQ) {
      cl = charlit_len(s, i)
      if (cl > 0) { CODE = CODE spaces(cl); FULL = FULL spaces(cl); i += cl; continue }
      CODE = CODE c; FULL = FULL c; i++; continue
    }
    if ((c == "r" || (c == "b" && substr(s, i + 1, 1) == "r")) && !is_ident(substr(s, i - 1, 1))) {
      j = i
      if (c == "b") j++
      k = j + 1
      hashes = 0
      while (substr(s, k, 1) == "#") { hashes++; k++ }
      if (substr(s, k, 1) == "\"") {
        sstate = 2; shashes = hashes
        CODE = CODE substr(s, i, k - i) OQ; FULL = FULL substr(s, i, k - i) OQ
        i = k + 1; continue
      }
    }
    if (c == "\"") { sstate = 1; CODE = CODE OQ; FULL = FULL OQ; i++; continue }
    CODE = CODE c; FULL = FULL c; i++
  }
}
function is_ident(c) {
  return c ~ /^[A-Za-z0-9_]$/
}
# The marker counts only from comment text, and only with a reason after it.
function has_marker(s,   i, rest) {
  i = index(s, MARKER)
  if (i == 0) return 0
  rest = substr(s, i + length(MARKER))
  gsub(/[ \t]/, "", rest)
  return length(rest) > 0
}
function open_span(name, allowed) {
  anchors++
  sp++
  sp_name[sp] = name
  sp_file[sp] = FILENAME
  sp_line[sp] = FNR
  sp_depth[sp] = depth
  sp_code[sp] = ""
  sp_full[sp] = ""
  sp_allowed[sp] = allowed || (sp > 1 && sp_allowed[sp - 1])
}
# Macro and method names are read from the code, so a string that mentions
# them is not a finding; an interpolation is read from the full text, since a
# format string is where it lives.
function close_span(   code, leak) {
  code = sp_code[sp]
  gsub(LITERAL_TO_STRING, "", code)
  leak = (code ~ /format(_args)?!/ || code ~ /to_string/ ||
          sp_full[sp] ~ /\{(error|err|e)(:[^}]*)?\}/)
  if (sp_name[sp] == "json!" && index(sp_full[sp], OQ "reason" CQ) == 0) leak = 0
  if (leak && !sp_allowed[sp]) {
    printf "%s:%d: error-display: the arguments of %s( are formatted text, which can" \
           " carry an internal error'"'"'s Display to the MCP caller; return a fixed" \
           " message and log the cause with d4::log_redacted (issue #4292), or mark" \
           " the call with `// %s <reason>`\n", \
           sp_file[sp], sp_line[sp], sp_name[sp], MARKER
    findings++
  } else if (leak && LIST_ALLOWED) {
    printf "%s:%d: allowed: %s( with formatted arguments, under a reasoned marker\n", \
           sp_file[sp], sp_line[sp], sp_name[sp]
  }
  sp--
}
function close_all() {
  while (sp > 0) close_span()
}
BEGIN {
  SQ = sprintf("%c", 39)
  OQ = sprintf("%c", 2)
  CQ = sprintf("%c", 3)
  # `"fixed".to_string()` and the raw-string form: a literal is no error value.
  LITERAL_TO_STRING = OQ "[^" CQ "]*" CQ "#*[ \t]*\\.[ \t]*to_string[ \t]*\\([ \t]*\\)"
  MARKER = "mcp-error-display-allow:"
  findings = 0
  anchors = 0
  sp = 0
}
FNR == 1 {
  close_all()
  sstate = 0; sesc = 0; shashes = 0; bdepth = 0
  depth = 0; block_marker = 0; pending_tests = 0; in_tests = 0
}
in_tests { next }
{
  strip_line($0)

  if (pending_tests) {
    if (CODE ~ /^[ \t]*(pub(\([^)]*\))?[ \t]+)?mod[ \t]+tests([ \t{;]|$)/) {
      close_all()
      in_tests = 1
      next
    }
    if (CODE !~ /^[ \t]*$/ && CODE !~ /^[ \t]*#!?\[/) pending_tests = 0
  }
  if (CODE ~ /^[ \t]*#\[cfg\(test\)\][ \t]*$/) pending_tests = 1

  line_allowed = has_marker(cmt) || block_marker
  n = length(CODE)
  for (i = 1; i <= n; i++) {
    c = substr(CODE, i, 1)
    if ((c == "d" || c == "e" || c == "M" || c == "j") && !is_ident(substr(CODE, i - 1, 1))) {
      rest = substr(CODE, i)
      name = ""
      if (match(rest, /^(d4|envelope)::(internal|failure)[ \t]*\(/)) name = substr(rest, 1, RLENGTH - 1)
      else if (match(rest, /^McpError::[a-z_]+[ \t]*\(/)) name = substr(rest, 1, RLENGTH - 1)
      else if (match(rest, /^json![ \t]*\(/)) name = "json!"
      if (name != "") {
        sub(/[ \t]+$/, "", name)
        open_span(name, line_allowed)
      }
    }
    for (k = 1; k <= sp; k++) {
      sp_code[k] = sp_code[k] c
      sp_full[k] = sp_full[k] substr(FULL, i, 1)
    }
    if (c == "(") depth++
    else if (c == ")") {
      depth--
      while (sp > 0 && sp_depth[sp] == depth) close_span()
    }
  }
  for (k = 1; k <= sp; k++) { sp_code[k] = sp_code[k] "\n"; sp_full[k] = sp_full[k] "\n" }

  # A comment block is contiguous: any non-comment line ends it, so a marker
  # cannot carry past the code it was written for.
  if ($0 ~ /^[ \t]*\/\//) {
    if (has_marker(cmt)) block_marker = 1
  } else {
    block_marker = 0
  }
}
END {
  close_all()
  if (anchors == 0) {
    printf "check-mcp-error-display.sh: none of d4::internal(, d4::failure(," \
           " McpError::<constructor>( or json!( appears in the scanned sources," \
           " so this scan cannot refuse anything; point it at the MCP failure" \
           " sites again (issue #4292)\n" > "/dev/stderr"
    exit 2
  }
  if (findings > 0) exit 1
  exit 0
}
'

LC_ALL=C awk -v LIST_ALLOWED="${list_allowed}" "${awk_prog}" "${sources[@]}"
rc=$?
if [[ ${rc} -eq 0 ]]; then
  echo "check-mcp-error-display.sh: no MCP failure is built from formatted error text"
fi
exit ${rc}
