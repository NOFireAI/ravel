#!/usr/bin/env bash
# Injected-clock test-helper guard (issue #1260, follow-up from #1235).
#
# "No wall-clock wait in an injected-clock test helper" cost two gate reruns
# (the flaky pair test, then its rewrite under #1235). That rule is now a
# check, not a paragraph: this scans every injected-clock test helper in
# services/ravel-cli/src/load.rs for thread::sleep, tokio::time::sleep,
# a bare/aliased sleep() call, tokio::time::timeout, Instant::, .elapsed()
# and SystemTime, and fails on a hit.
#
# Scan span and predicate. The whole `#[cfg(test)]` module (from the first
# `#[cfg(test)]` line to end of file) is the scan region, not a hand-picked
# set of helper spans. Inside it, a function item is scanned when its
# signature or body mentions one of the injected-clock types named once in
# CLOCK_TYPES below (TestClock, FixedClock), plus the two helpers issue #1260
# names by name (load_two_writes_across_one_clock_advance,
# load_with_released_tail) so a rename of a clock type cannot silently empty
# the scan. A scan that finds zero helpers is itself a failure -- see below.
#
# Not covered, on purpose: a helper that delegates its wait to another
# function which never names a clock type. The scan keys on the clock type
# appearing in the function that also holds the wall-clock construct; a wait
# hidden one call deep in a clock-free helper is out of scope. Widening to it
# would need a call graph, which this source-only scan does not build.
#
# Injected-clock receivers are not wall-clock waits. `clock.sleep(..).await`
# and `clock.elapsed()` are the idiom this guard exists to encourage, so a
# `.sleep(` / `.elapsed(` call is masked when its receiver chain names an
# injected clock. The test is name-based, since a source-only scan has no
# types: the chain has to mention one of CLOCK_TYPES or an identifier
# containing "clock" in either case. Any other receiver (`start.elapsed()`,
# `timer.sleep(..)`) still reports.
#
# Allowlist: a single trailing comment marker on the offending line,
# `// allow-wall-clock: <reason>`, with a non-empty reason and nothing else
# (no env var, no exempt file). The marker is matched on the string-stripped
# line, so a string literal that merely contains the marker text cannot
# suppress a real finding.
#
# Helper-count floor on the default target. The zero-helpers guard below
# catches a predicate that finds nothing at all, but not one that narrows: as
# shipped the real file scans 26 helpers, and dropping FixedClock from
# CLOCK_TYPES takes that to 5 (dropping both clock types, to 2) while still
# exiting 0. So a scan of the default target that finds fewer than
# DEFAULT_TARGET_MIN_HELPERS helpers fails. It is a floor rather than an exact
# count on purpose: a new injected-clock helper in load.rs must never fail the
# gate, only a predicate that stops seeing the ones already there.
#
# Scope note. This guard scans one file, services/ravel-cli/src/load.rs. It is
# not yet enforced workspace-wide: other crates hold injected-clock tests it
# does not read (see CLAUDE.md for the measured out-of-scope counts).
#
# Usage:
#   scripts/check-injected-clock-helpers.sh [file]
#     file defaults to services/ravel-cli/src/load.rs (relative to the repo
#     root). Tests pass a throwaway fixture path here instead.
#
# Exit 0 clean, 1 on a wall-clock hit, on a zero-helpers scan, or on a
# default-target scan under the helper-count floor, 64 on bad usage, 70 if the
# scan itself could not run (unreadable file).
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

case "${1:-}" in
  -h | --help)
    sed -n '2,60p' "$0"
    exit 0
    ;;
esac

if [[ $# -gt 1 ]]; then
  echo "check-injected-clock-helpers.sh: unexpected extra argument: $2" >&2
  exit 64
fi

default_target="${repo_root}/services/ravel-cli/src/load.rs"
target="${1:-${default_target}}"

if [[ ! -f "${target}" ]]; then
  echo "check-injected-clock-helpers.sh: no such file: ${target}" >&2
  exit 64
fi

# The helper-count floor applies to the default target only (see the header):
# a fixture legitimately holds a handful of helpers. `-ef` compares the files
# themselves, so a relative path or a symlink to load.rs is still the default
# target.
is_default=0
if [[ "${target}" -ef "${default_target}" ]]; then
  is_default=1
fi
DEFAULT_TARGET_MIN_HELPERS=20

# --- the scan ---------------------------------------------------------------
#
# Single-file scan, so no per-file state reset is needed (unlike
# check-test-hygiene.sh, which processes many files through one awk pass).
#
# strip_both computes two views of each line in one walk that carries string
# and block-comment state across lines:
#   clean[] -- comment AND string/char-literal content removed. Used for the
#     `fn` keyword, the clock-type mention, and the wall-clock symbols, so
#     none of them ever match inside a comment or a string.
#   nostr[] -- string/char-literal content removed but comments KEPT. Used
#     only for the allow-wall-clock marker, which is itself a trailing
#     comment: clean[] would erase it, and the raw line would let a string
#     literal containing the marker text suppress a real finding.
#
# A function's body span is found by paren/brace counting on the clean text,
# starting from column 1 of its `fn` line: any parens in a `pub(crate)`
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
# Walk s once, setting globals CLEAN (code only) and NOSTR (code + comments,
# strings blanked). String/block-comment state (bdepth, sstate, sesc,
# shashes) is global on purpose so a multi-line string or block comment is
# tracked across successive calls.
function strip_both(s,   i, n, c, j, k, hashes, hs, cl) {
  CLEAN = ""; NOSTR = ""; n = length(s); i = 1
  while (i <= n) {
    c = substr(s, i, 1)
    if (bdepth > 0) {
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; NOSTR = NOSTR "/*"; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; NOSTR = NOSTR "*/"; i += 2; continue }
      NOSTR = NOSTR c; i++; continue
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
    if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth = 1; NOSTR = NOSTR "/*"; i += 2; continue }
    if (c == "/" && substr(s, i + 1, 1) == "/") { NOSTR = NOSTR substr(s, i); break }
    if (c == SQ) {
      cl = charlit_len(s, i)
      if (cl > 0) { i += cl; continue }
      CLEAN = CLEAN c; NOSTR = NOSTR c; i++; continue
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
    CLEAN = CLEAN c; NOSTR = NOSTR c; i++
  }
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
# True when a receiver chain names an injected clock: it mentions one of
# CLOCK_TYPES, or an identifier containing "clock" in either case. Name-based
# because this scan has no types; see the header.
function is_clock_receiver(chain,   ci) {
  if (tolower(chain) ~ /clock/) return 1
  for (ci = 1; ci <= nct; ci++) {
    if (index(chain, clock_types[ci]) > 0) return 1
  }
  return 0
}
# Rewrite `<recv>.sleep(` / `<recv>.elapsed(` to an inert method name when
# <recv> is an injected clock, so awaiting the clock this guard exists to
# encourage is not itself reported as a wall-clock wait. Only the two
# receiver-form patterns can be masked: `thread::sleep`, `tokio::time::sleep`,
# `tokio::time::timeout`, `Instant::` and `SystemTime` carry no leading dot,
# and masking is per call, so a real finding elsewhere on the line still
# reports.
function mask_clock_calls(s,   out, rest, pos, len, base, chain) {
  out = ""
  rest = s
  while (1) {
    pos = match(rest, /\.(sleep|elapsed)[ \t]*\(/)
    if (pos == 0) return out rest
    len = RLENGTH
    base = substr(rest, 1, pos - 1)
    chain = base
    sub(/^.*[^A-Za-z0-9_:.()&*]/, "", chain)
    if (is_clock_receiver(chain)) out = out base ".injected_clock_call("
    else out = out base substr(rest, pos, len)
    rest = substr(rest, pos + len)
  }
}
BEGIN { SQ = sprintf("%c", 39) }
{
  nlines++
  strip_both($0)
  raw[nlines] = $0
  clean[nlines] = CLEAN
  nostr[nlines] = NOSTR
  # Anchored on the stripped line like every other predicate here: a doc
  # comment or string literal mentioning the attribute would otherwise start
  # the scan above the real test module and pull production functions in.
  if (cfg_test_line == 0 && CLEAN ~ /#\[cfg\(test\)\]/) cfg_test_line = nlines
}
END {
  if (nlines == 0) {
    print "check-injected-clock-helpers.sh: " FILENAME " is empty" > "/dev/stderr"
    exit 70
  }
  scan_start = (cfg_test_line > 0) ? cfg_test_line : nlines + 1

  NAMED_1 = "load_two_writes_across_one_clock_advance"
  NAMED_2 = "load_with_released_tail"

  # The injected-clock types, named once. A function that mentions any of
  # these (or is one of the two named helpers) is scanned.
  nct = 0
  clock_types[++nct] = "TestClock"
  clock_types[++nct] = "FixedClock"

  # Wall-clock constructs, matched as regexes on the clean line. First match
  # per line wins, so the qualified sleep names report before the bare
  # sleep() pattern (which exists to catch an aliased `use ...::sleep`).
  np = 0
  pname[++np] = "thread::sleep";        pre[np] = "thread::sleep"
  pname[++np] = "tokio::time::sleep";   pre[np] = "tokio::time::sleep"
  pname[++np] = "tokio::time::timeout"; pre[np] = "tokio::time::timeout"
  pname[++np] = "Instant::";            pre[np] = "Instant::"
  pname[++np] = "SystemTime";           pre[np] = "SystemTime"
  pname[++np] = ".elapsed()";           pre[np] = "\\.elapsed\\(\\)"
  pname[++np] = "sleep()";              pre[np] = "(^|[^A-Za-z0-9_:])sleep[ \t]*\\("

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
    for (k = i; k <= end_line && !mentions; k++) {
      for (ci = 1; ci <= nct; ci++) {
        if (index(clean[k], clock_types[ci]) > 0) { mentions = 1; break }
      }
    }
    is_named = (name == NAMED_1 || name == NAMED_2)

    if (mentions || is_named) {
      helper_count++
      for (k = i; k <= end_line; k++) {
        masked = mask_clock_calls(clean[k])
        sym = ""
        for (pi = 1; pi <= np; pi++) {
          if (masked ~ pre[pi]) { sym = pname[pi]; break }
        }
        if (sym == "") continue
        # allow-wall-clock marker, matched on the string-stripped line and
        # requiring a non-empty reason.
        if (nostr[k] ~ /\/\/ allow-wall-clock:[ \t]*[^ \t]/) continue
        finding_count++
        printf "%s:%d: %s in %s\n", FILENAME, k, sym, name
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
      "a clock type renamed, or the named helpers (%s / %s) removed?\n", \
      FILENAME, NAMED_1, NAMED_2 > "/dev/stderr"
    exit 1
  }
  if (is_default && helper_count < min_helpers) {
    printf "check-injected-clock-helpers.sh: only %d injected-clock helper(s) scanned in %s, " \
      "under the floor of %d -- did the helper predicate narrow (a clock type dropped from " \
      "CLOCK_TYPES)?\n", helper_count, FILENAME, min_helpers > "/dev/stderr"
    exit 1
  }
  printf "check-injected-clock-helpers.sh: clean (%d helper(s) scanned)\n", helper_count
  exit 0
}
'

scan_status=0
awk -v is_default="${is_default}" -v min_helpers="${DEFAULT_TARGET_MIN_HELPERS}" \
  "${awk_prog}" "${target}" || scan_status=$?
if [[ ${scan_status} -gt 1 ]]; then
  echo "check-injected-clock-helpers.sh: the scan failed (awk exit ${scan_status})" >&2
  exit 70
fi
exit "${scan_status}"
