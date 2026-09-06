#!/usr/bin/env bash
# Injected-clock test-helper guard (issue #1260, follow-up from #1235).
#
# "No wall-clock wait in an injected-clock test helper" cost two gate reruns
# (the flaky pair test, then its rewrite under #1235). That rule is now a
# check, not a paragraph: this scans every injected-clock test helper in
# services/ravel-cli/src/load.rs for thread::sleep, tokio::time::sleep,
# Instant::, tokio::time::timeout and SystemTime, and fails on a hit.
#
# The scanned region is derived mechanically, not from a hand-maintained line
# list: every function item in the `#[cfg(test)]` module whose signature or
# body mentions `TestClock` is scanned, plus the two helpers issue #1260 names
# by name (load_two_writes_across_one_clock_advance, load_with_released_tail)
# so a rename of the TestClock type cannot silently empty the scan. A scan
# that finds zero helpers is itself a failure -- see below.
#
# Allowlist: a single trailing comment marker on the offending line,
# `// allow-wall-clock: <reason>`, and nothing else (no env var, no exempt
# file).
#
# Usage:
#   scripts/check-injected-clock-helpers.sh [file]
#     file defaults to services/ravel-cli/src/load.rs (relative to the repo
#     root). Tests pass a throwaway fixture path here instead.
#
# Exit 0 clean, 1 on a wall-clock hit or on a zero-helpers scan, 64 on bad
# usage, 70 if the scan itself could not run (unreadable file).
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

case "${1:-}" in
  -h | --help)
    sed -n '2,26p' "$0"
    exit 0
    ;;
esac

if [[ $# -gt 1 ]]; then
  echo "check-injected-clock-helpers.sh: unexpected extra argument: $2" >&2
  exit 64
fi

target="${1:-${repo_root}/services/ravel-cli/src/load.rs}"

if [[ ! -f "${target}" ]]; then
  echo "check-injected-clock-helpers.sh: no such file: ${target}" >&2
  exit 64
fi

# --- the scan ---------------------------------------------------------------
#
# Single-file scan, so no per-file state reset is needed (unlike
# check-test-hygiene.sh, which processes many files through one awk pass).
#
# strip_line/charlit_len strip comment and string-literal content so the
# scan for `TestClock`, the five wall-clock symbols, and the `fn` keyword
# itself never matches inside a doc comment or a string; the allow-wall-clock
# marker is deliberately checked against the RAW line instead, because it is
# itself a trailing comment that strip_line would otherwise erase.
#
# A function's body span is found by paren/brace counting on the stripped
# text, starting from column 1 of its `fn` line: any parens in a `pub(crate)`
# prefix close before `fn` is reached, so paren depth is already back at 0 by
# the time the real argument list opens, and the first top-level `{` after
# that is unambiguously the body open. This needs no argument-list special
# case and no generic-bracket tracking, because Rust never leaves a `(` open
# across the function signature into the body.
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
  out = ""; n = length(s); i = 1
  while (i <= n) {
    c = substr(s, i, 1)
    if (bdepth > 0) {
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; i += 2; continue }
      i++; continue
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
    if (c == "/" && substr(s, i + 1, 1) == "/") break
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
      if (substr(s, k, 1) == "\"") {
        sstate = 2; shashes = hashes; i = k + 1; continue
      }
    }
    if (c == "\"") { sstate = 1; i++; continue }
    out = out c; i++
  }
  return out
}
# Returns the function name if `line` (already comment/string-stripped) is
# the start of a `fn` item -- optionally preceded by pub()/async/unsafe/
# const/extern modifiers -- or "" otherwise.
function fn_candidate(line,   t, i, n, c) {
  t = line
  sub(/^[ \t]+/, "", t)
  while (1) {
    if (t ~ /^pub\(crate\)[ \t]+/)  { sub(/^pub\(crate\)[ \t]+/, "", t); continue }
    if (t ~ /^pub\(super\)[ \t]+/)  { sub(/^pub\(super\)[ \t]+/, "", t); continue }
    if (t ~ /^pub\(self\)[ \t]+/)   { sub(/^pub\(self\)[ \t]+/, "", t); continue }
    if (t ~ /^pub[ \t]+/)           { sub(/^pub[ \t]+/, "", t); continue }
    if (t ~ /^async[ \t]+/)         { sub(/^async[ \t]+/, "", t); continue }
    if (t ~ /^unsafe[ \t]+/)        { sub(/^unsafe[ \t]+/, "", t); continue }
    if (t ~ /^const[ \t]+/)         { sub(/^const[ \t]+/, "", t); continue }
    if (t ~ /^extern[ \t]+"[A-Za-z]+"[ \t]+/) {
      sub(/^extern[ \t]+"[A-Za-z]+"[ \t]+/, "", t); continue
    }
    break
  }
  if (t !~ /^fn[ \t]+[A-Za-z_][A-Za-z0-9_]*/) return ""
  sub(/^fn[ \t]+/, "", t)
  n = length(t); i = 1
  while (i <= n) {
    c = substr(t, i, 1)
    if (c !~ /[A-Za-z0-9_]/) break
    i++
  }
  return substr(t, 1, i - 1)
}
BEGIN { SQ = sprintf("%c", 39) }
{
  nlines++
  raw[nlines] = $0
  clean[nlines] = strip_line($0)
  if (cfg_test_line == 0 && $0 ~ /#\[cfg\(test\)\]/) cfg_test_line = nlines
}
END {
  if (nlines == 0) {
    print "check-injected-clock-helpers.sh: " FILENAME " is empty" > "/dev/stderr"
    exit 70
  }
  scan_start = (cfg_test_line > 0) ? cfg_test_line : nlines + 1

  NAMED_1 = "load_two_writes_across_one_clock_advance"
  NAMED_2 = "load_with_released_tail"

  nsyms = 0
  syms[++nsyms] = "thread::sleep"
  syms[++nsyms] = "tokio::time::sleep"
  syms[++nsyms] = "Instant::"
  syms[++nsyms] = "tokio::time::timeout"
  syms[++nsyms] = "SystemTime"

  helper_count = 0
  finding_count = 0

  i = scan_start
  while (i <= nlines) {
    name = fn_candidate(clean[i])
    if (name == "") { i++; continue }

    # Find the body-opening brace: scan from column 1 of the fn line,
    # tracking paren depth, for the first "{" at depth 0. A ";" at depth 0
    # first means a body-less signature (a trait method) -- skip it.
    pd = 0; l = i; c = 1
    found_open = 0; no_body = 0
    body_line = 0; body_col = 0
    while (l <= nlines && !found_open && !no_body) {
      s = clean[l]; n = length(s)
      while (c <= n) {
        ch = substr(s, c, 1)
        if (ch == "(") pd++
        else if (ch == ")") pd--
        else if (ch == "{" && pd == 0) { body_line = l; body_col = c; found_open = 1; break }
        else if (ch == ";" && pd == 0) { no_body = 1; break }
        c++
      }
      if (!found_open && !no_body) { l++; c = 1 }
    }
    if (no_body || !found_open) { i++; continue }

    bd = 1; l2 = body_line; c2 = body_col + 1; end_line = 0
    while (l2 <= nlines && bd > 0) {
      s = clean[l2]; n = length(s)
      while (c2 <= n) {
        ch = substr(s, c2, 1)
        if (ch == "{") bd++
        else if (ch == "}") { bd--; if (bd == 0) break }
        c2++
      }
      if (bd == 0) { end_line = l2; break }
      l2++; c2 = 1
    }
    if (end_line == 0) { i++; continue }

    mentions = 0
    for (k = i; k <= end_line; k++) {
      if (index(clean[k], "TestClock") > 0) { mentions = 1; break }
    }
    is_named = (name == NAMED_1 || name == NAMED_2)

    if (mentions || is_named) {
      helper_count++
      for (k = i; k <= end_line; k++) {
        for (si = 1; si <= nsyms; si++) {
          if (index(clean[k], syms[si]) > 0) {
            if (index(raw[k], "// allow-wall-clock:") > 0) continue
            finding_count++
            printf "%s:%d: %s in %s\n", FILENAME, k, syms[si], name
          }
        }
      }
    }

    i = end_line + 1
  }

  if (finding_count > 0) {
    printf "check-injected-clock-helpers.sh: %d finding(s) across %d scanned helper(s)\n", \
      finding_count, helper_count > "/dev/stderr"
    exit 1
  }
  if (helper_count == 0) {
    printf "check-injected-clock-helpers.sh: 0 injected-clock helpers scanned in %s -- " \
      "TestClock renamed, or the named helpers (%s / %s) removed?\n", \
      FILENAME, NAMED_1, NAMED_2 > "/dev/stderr"
    exit 1
  }
  printf "check-injected-clock-helpers.sh: clean (%d helper(s) scanned)\n", helper_count
  exit 0
}
'

scan_status=0
awk "${awk_prog}" "${target}" || scan_status=$?
if [[ ${scan_status} -gt 1 ]]; then
  echo "check-injected-clock-helpers.sh: the scan failed (awk exit ${scan_status})" >&2
  exit 70
fi
exit "${scan_status}"
