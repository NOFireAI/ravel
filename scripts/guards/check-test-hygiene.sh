#!/usr/bin/env bash
# Test-hygiene guard: the three defect shapes CLAUDE.md names under "Testing
# patterns" as ones that bit twice and therefore become a check rather than
# another paragraph.
#
#   wall-clock    an assertion whose subject is elapsed real time. Flaky on a
#                 loaded box, vacuous on a fast one. Library logic takes a
#                 Clock or a now_ns parameter so tests are deterministic.
#   unseeded-rng  a test drawing from OS entropy. Fails once in a hundred runs
#                 and is then re-run until green, which is how a real defect
#                 gets attributed to flakiness.
#   proptest-seed a proptest regression seed that exists on disk but is not
#                 tracked by git. proptest writes one when it catches a case;
#                 uncommitted, the default test command never replays it and
#                 the same defect is found again at gate time by someone else.
#
# Usage:
#   scripts/guards/check-test-hygiene.sh [path ...]   # default: crates services
#
# Exit 0 clean, 1 on findings, 64 on bad usage. Findings print as
# `file:line: rule: explanation`.
#
# Escape hatch, per finding, on the flagged line or anywhere in the comment
# block immediately above it:
#
#   // hygiene-allow: wall-clock -- <reason>
#   // hygiene-allow: unseeded-rng -- <reason>
#
# The reason is for the next reader; the guard only looks for the marker. Use
# it for a case that genuinely cannot be converted (an #[ignore]d measurement
# probe whose whole subject is wall time), not to quiet a test that could take
# an injected clock.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,32p' "$0"
      exit 0
      ;;
    -*)
      echo "check-test-hygiene.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=(crates services)
fi

findings_file="$(mktemp "${TMPDIR:-/tmp}/ravel-test-hygiene.XXXXXX")"
sources_file="$(mktemp "${TMPDIR:-/tmp}/ravel-test-hygiene-src.XXXXXX")"
trap 'rm -f "${findings_file}" "${sources_file}"' EXIT

find "${roots[@]}" -type f -name '*.rs' \
  -not -path '*/target/*' \
  -not -path '*/node_modules/*' \
  | sort >"${sources_file}"

if [[ ! -s "${sources_file}" ]]; then
  echo "check-test-hygiene.sh: no Rust sources under ${roots[*]}" >&2
  exit 64
fi

# --- rules 1 and 2 ---------------------------------------------------------
#
# Both need whole-file context, so they share one awk pass per file.
#
# The wall-clock rule propagates through assignment: `let d = t0.elapsed();`
# makes `d` a timing identifier, `let share = d / total;` makes `share` one too,
# and an assertion naming either is a wall-clock assertion even though it never
# says `elapsed` itself. That is the usual shape -- the measurement and the
# assertion sit twenty lines apart with a ratio in between.
#
# Both the measurement and the assertion must be in test code for the rule to
# fire. Production timing code shares identifier names with the report fields a
# test later asserts the mere presence of, and tainting those names repo-wide
# turns a presence check into a reported timing band.
#
# Propagation is by name within a file, not by scope: a name bound from a
# measurement in one test and reused for something unrelated in another test in
# the same file is a false positive. The escape-hatch marker covers it.
awk_prog='
function strip_line(s,   out, i, c, n, instr, esc) {
  # Drop // comments and double-quoted string contents, so paren counting and
  # identifier matching never see an assertion message.
  out = ""; instr = 0; esc = 0; n = length(s)
  for (i = 1; i <= n; i++) {
    c = substr(s, i, 1)
    if (instr) {
      if (esc) { esc = 0; continue }
      if (c == "\\") { esc = 1; continue }
      if (c == "\"") { instr = 0 }
      continue
    }
    if (c == "\"") { instr = 1; continue }
    if (c == "/" && substr(s, i + 1, 1) == "/") break
    out = out c
  }
  return out
}
function balance(s,   i, n, c, b) {
  b = 0; n = length(s)
  for (i = 1; i <= n; i++) {
    c = substr(s, i, 1)
    if (c == "(") b++
    else if (c == ")") b--
  }
  return b
}
function has_word(s, w) {
  return s ~ ("(^|[^A-Za-z0-9_])" w "([^A-Za-z0-9_]|$)")
}
function allowed(rule, line,   i) {
  # The marker sits on the flagged line, or anywhere in the contiguous comment
  # block immediately above it. Walking the block rather than a fixed number of
  # lines is what lets the reason be as long as it needs to be.
  if (index(raw[line], "hygiene-allow: " rule) > 0) return 1
  for (i = line - 1; i >= 1; i--) {
    if (raw[i] !~ /^[ \t]*\/\//) return 0
    if (index(raw[i], "hygiene-allow: " rule) > 0) return 1
  }
  return 0
}
function report(rule, line, why) {
  printf "%s:%d: %s: %s\n", curfile, line, rule, why
}
function in_test(line) {
  # Test code: a file under tests/ or benches/, a tests.rs module file, or
  # anything below the first #[cfg(test)] in a source file (test modules sit at
  # the bottom by convention here).
  return is_test_file || (cfg_test_line > 0 && line >= cfg_test_line)
}
function scan(   i, j, buf, rawbuf, start, b, id, lhs, rhs, k, parts, ntok, cap, hit, changed, rounds, src) {
  # Pass A: identifiers holding a wall-clock measurement. Test regions only:
  # production timing code (a stage timer, a latency histogram feed) shares
  # names with the fields a test later asserts the mere presence of, and
  # tainting those names repo-wide flags a presence check as a timing band.
  for (i = 1; i <= nlines; i++) {
    if (!in_test(i)) continue
    if (index(clean[i], ".elapsed()") == 0) continue
    if (index(clean[i], "=") == 0) continue
    lhs = substr(clean[i], 1, index(clean[i], "=") - 1)
    if (lhs ~ /[!<>=]$/) continue          # a comparison, not an assignment
    gsub(/[^A-Za-z0-9_]+/, " ", lhs)
    ntok = split(lhs, parts, " ")
    id = ""
    if (ntok >= 1 && parts[1] == "let") {
      for (k = 2; k <= ntok; k++) {
        if (parts[k] == "mut") continue
        id = parts[k]
        break
      }
    } else if (ntok >= 1) {
      id = parts[ntok]
    }
    if (id != "" && id !~ /^[0-9]/) timing[id] = 1
  }
  # Pass A2: propagate the taint through derived bindings, to a fixpoint. A
  # measurement is usually not asserted on raw; it is turned into a ratio or a
  # difference first (`let share = clone_ns / total;`) and that is what the
  # assertion names.
  changed = 1
  rounds = 0
  while (changed && rounds < 8) {
    changed = 0; rounds++
    for (i = 1; i <= nlines; i++) {
      if (!in_test(i)) continue
      if (clean[i] !~ /^[ \t]*let[ \t]/) continue
      if (index(clean[i], "=") == 0) continue
      lhs = substr(clean[i], 1, index(clean[i], "=") - 1)
      rhs = substr(clean[i], index(clean[i], "=") + 1)
      if (lhs ~ /[!<>=]$/) continue
      gsub(/[^A-Za-z0-9_]+/, " ", lhs)
      ntok = split(lhs, parts, " ")
      id = ""
      for (k = 2; k <= ntok; k++) {
        if (parts[k] == "mut") continue
        id = parts[k]
        break
      }
      if (id == "" || id in timing) continue
      for (src in timing) {
        if (has_word(rhs, src)) { timing[id] = 1; changed = 1; break }
      }
    }
  }
  # Pass B: assertion blocks.
  for (i = 1; i <= nlines; i++) {
    if (!in_test(i)) continue
    if (clean[i] !~ /assert[a-z_]*!/) continue
    if (index(clean[i], "(") == 0) continue
    start = i
    buf = substr(clean[i], index(clean[i], "assert"))
    rawbuf = raw[i]
    b = balance(buf)
    cap = 0
    j = i
    # An unbalanced capture (a raw string carrying a stray paren, a macro split
    # oddly) is abandoned rather than guessed at: no finding beats a wrong one.
    while (b > 0 && cap < 40 && j < nlines) {
      j++; cap++
      buf = buf " " clean[j]
      rawbuf = rawbuf "\n" raw[j]
      b = balance(buf)
    }
    if (b != 0) continue
    i = j
    if (index(rawbuf, "hygiene-allow: wall-clock") > 0 || allowed("wall-clock", start)) continue
    if (index(buf, ".elapsed()") > 0) {
      report("wall-clock", start, "assertion reads .elapsed(): assert on injected time, or state why real time is the subject")
      continue
    }
    hit = ""
    for (id in timing) {
      if (has_word(buf, id)) { hit = id; break }
    }
    if (hit != "") {
      report("wall-clock", start, "assertion reads `" hit "`, which holds a .elapsed() measurement")
    }
  }
  # Pass C: entropy draws in test code.
  for (i = 1; i <= nlines; i++) {
    if (!in_test(i)) continue
    if (clean[i] !~ /rand::rng\(\)|rand::random|thread_rng\(\)|OsRng|from_entropy\(\)|SystemRandom::new\(\)|getrandom/) continue
    if (allowed("unseeded-rng", i)) continue
    report("unseeded-rng", i, "test draws from OS entropy: seed it (StdRng::seed_from_u64) so a failure replays")
  }
}
FNR == 1 {
  if (nlines > 0) scan()
  delete raw; delete clean; delete timing
  nlines = 0; cfg_test_line = 0
  curfile = FILENAME
  is_test_file = (FILENAME ~ /\/tests\// || FILENAME ~ /\/benches\// || FILENAME ~ /tests\.rs$/)
}
{
  nlines++
  raw[nlines] = $0
  clean[nlines] = strip_line($0)
  if (cfg_test_line == 0 && $0 ~ /#\[cfg\(test\)\]/) cfg_test_line = nlines
}
END { if (nlines > 0) scan() }
'
xargs -a "${sources_file}" awk "${awk_prog}" >"${findings_file}"

# --- rule 3 ----------------------------------------------------------------
#
# proptest's default persistence writes a caught seed next to the source:
# `<crate>/proptest-regressions/<path under src>.txt` for a property test in
# `src/`, and `<dir>/<name>.proptest-regressions` for one under `tests/`. Both
# forms exist in this repo. A seed that exists but is untracked, or that a
# .gitignore covers, is never replayed by `cargo test`.
while IFS= read -r src; do
  grep -q 'proptest!' "${src}" || continue
  case "${src}" in
    */src/*)
      crate_root="${src%%/src/*}"
      rel="${src#"${crate_root}"/src/}"
      seed="${crate_root}/proptest-regressions/${rel%.rs}.txt"
      ;;
    *)
      seed="${src%.rs}.proptest-regressions"
      ;;
  esac
  [[ -e "${seed}" ]] || continue
  # --no-index on purpose: without it git reports nothing for a file already in
  # the index, so a rule that will silently swallow the NEXT seed written here
  # stays invisible until it does.
  if git check-ignore --no-index -q "${seed}" 2>/dev/null; then
    echo "${seed}:1: proptest-seed: a .gitignore rule covers this path; regression seeds must be committable" >>"${findings_file}"
  elif ! git ls-files --error-unmatch "${seed}" >/dev/null 2>&1; then
    echo "${seed}:1: proptest-seed: exists on disk but is untracked; git add it or ${src}'s caught case is never replayed" >>"${findings_file}"
  fi
done <"${sources_file}"

# A `failure_persistence` override can switch seed writing off entirely, and
# then rule 3 has no file to find. Flag every occurrence: there is no
# legitimate use of it here.
xargs -a "${sources_file}" grep -Hn 'failure_persistence' 2>/dev/null \
  | sed 's/^\([^:]*:[0-9]*\):.*$/\1: proptest-seed: failure_persistence override can disable seed writing; the caught case is then lost/' \
    >>"${findings_file}"

count=$(grep -c '' "${findings_file}")
if [[ "${count}" -eq 0 ]]; then
  echo "check-test-hygiene.sh: clean ($(grep -c '' "${sources_file}") Rust sources)"
  exit 0
fi

sort -t: -k1,1 -k2,2n "${findings_file}"
echo "check-test-hygiene.sh: ${count} finding(s)" >&2
exit 1
