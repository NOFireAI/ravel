#!/usr/bin/env bash
# Test-hygiene guard: five defect shapes. CLAUDE.md's "Testing patterns"
# names the first three (wall-clock, unseeded-rng, proptest-seed) as ones
# that bit twice and therefore became a check rather than another paragraph;
# ignore-flaky and clock-compared are two later additions this guard also
# tracks.
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
#   ignore-flaky  an #[ignore] whose reason (the `= "..."` string, a trailing
#                 `//` comment, or the comment block directly above it) names
#                 "flaky" but cites no issue. A flaky test that is silenced
#                 with no ticket is a defect nobody is tracking.
#   clock-compared a test that calls a fixture-building helper twice, where
#                 the helper reads the wall clock itself (`now_ns()` /
#                 `SystemTime::now()`, not a clock passed in), and both calls'
#                 results feed one assertion. Flaky across an hour or minute
#                 boundary (#4000): build both fixtures from one reading
#                 instead, passed in rather than read by the helper.
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
# an injected clock. ignore-flaky and clock-compared take no escape hatch: an
# ignore naming "flaky" always needs an issue, and a helper that reads the
# clock itself always needs to take one instead.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,44p' "$0"
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
  -print0 | sort -z >"${sources_file}"

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
# String and comment contents are prose, not code: paren counting and
# identifier matching must never see an assertion message. strip_line removes
# them and returns the code skeleton of one physical line.
#
# State (sstate/shashes/sesc) is per file, carried across lines, because a Rust
# string literal spans lines: an ordinary "..." continues until its closing
# quote (a trailing backslash-newline is just whitespace trimming, not a
# terminator), and a raw string r#"..."# can hold newlines outright. The old
# per-line stripper reset at every newline, so the second and later lines of a
# multi-line assertion message were scanned as if they were code -- that is the
# false positive this guard shipped (issue #896): the English word "wall" in a
# continued message matched a `wall` timing identifier. FNR==1 resets the state
# for each file.
#
# String forms handled: ordinary "..." with \" escapes, raw strings r"...",
# r#"..."# and higher hash counts, and the byte-string prefixes b"..." / br"..."
# / br#"..."#. Char literals are consumed whole so a quote inside one (a
# quote char literal, or an escaped-quote char literal) cannot open a spurious
# string that swallows the rest of the file. Order matters: // is checked before
# any quote, and a char literal and raw-string opener are recognised before a
# bare " opens an ordinary string.
function charlit_len(s, i,   c2, c, j) {
  # s[i] is a single quote. Return the length of a char literal starting there,
  # or 0 when the quote opens a lifetime/label (`\x27a`, `\x27static`) instead.
  c2 = substr(s, i + 1, 1)
  if (c2 == "\\") {
    j = i + 2                       # first char after the backslash
    c = substr(s, j, 1)
    if (c == "x") j = j + 3         # \xHH then the closing quote
    else if (c == "u") {            # \u{HHHH...}
      j = j + 1
      while (j <= length(s) && substr(s, j, 1) != "}") j++
      j++
    } else j = j + 1                # single-char escape (\n \\ \x27 \" ...)
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
    # Block comments first: inside one, quotes and `//` carry no meaning, so
    # this has to shadow the string and line-comment branches below. Rust
    # nests them, so this counts depth rather than scanning for the first
    # `*/`. bdepth persists across lines like sstate, because a block comment
    # opened on one line closes on another.
    if (bdepth > 0) {
      if (c == "/" && substr(s, i + 1, 1) == "*") { bdepth++; i += 2; continue }
      if (c == "*" && substr(s, i + 1, 1) == "/") { bdepth--; i += 2; continue }
      i++; continue
    }
    if (sstate == 1) {                          # inside "..."
      if (sesc) { sesc = 0; i++; continue }
      if (c == "\\") { sesc = 1; i++; continue }
      if (c == "\"") sstate = 0
      i++; continue
    }
    if (sstate == 2) {                          # inside raw string, shashes #s
      if (c == "\"") {
        hs = ""
        for (j = 0; j < shashes; j++) hs = hs "#"
        if (shashes == 0 || substr(s, i + 1, shashes) == hs) {
          sstate = 0; i += 1 + shashes; continue
        }
      }
      i++; continue
    }
    if (c == "/" && substr(s, i + 1, 1) == "*") {       # block comment opens
      bdepth = 1; i += 2; continue
    }
    if (c == "/" && substr(s, i + 1, 1) == "/") break   # line comment
    if (c == SQ) {                              # char literal or lifetime
      cl = charlit_len(s, i)
      if (cl > 0) { i += cl; continue }         # drop the literal
      out = out c; i++; continue                # a lifetime tick: keep it
    }
    if (c == "r" || (c == "b" && substr(s, i + 1, 1) == "r")) {
      j = i
      if (c == "b") j++                         # skip a byte-string prefix
      k = j + 1                                 # j is the r, k scans the hashes
      hashes = 0
      while (substr(s, k, 1) == "#") { hashes++; k++ }
      if (substr(s, k, 1) == "\"") {
        sstate = 2; shashes = hashes; i = k + 1; continue
      }
      # not a raw string (a raw identifier r#ident, or a plain r): fall through
    }
    if (c == "\"") { sstate = 1; i++; continue }
    out = out c; i++
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
# Call syntax, not a bare reference: the name immediately followed by "(". A
# plain word match on now_ns or clock would also hit a RHS that merely passes
# an already-injected now_ns parameter along, e.g. bucket.peek(amount, now_ns),
# which reads nothing from the clock itself.
function is_call(s, w) {
  return s ~ ("(^|[^A-Za-z0-9_])" w "\\(")
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
# A function signature names an injected clock: a parameter called exactly
# `now`/`clock`, or ending/starting with `_now`/`now_`/`_clock`/`clock_`. This
# is narrower than the #4000 fix itself: post-fix `serve(setup: Setup)`
# escapes find_clock_fns because its body reads
# `setup.now.unwrap_or_else(now_ns)`, which the unconditional-call pattern
# below never matches -- not because its signature carries a now/clock
# parameter (it carries none; `setup: Setup` matches nothing here). This
# check exists for the shape that pattern alone would miss: a helper whose
# signature takes the clock directly (`fn fixture(now: i64)`) but whose body
# still has a literal `let fallback = now_ns();` as a default for when the
# caller passes nothing. Without this exemption that body shape alone would
# mark the helper a clock-reader even though the caller already controls the
# time.
function fn_sig_has_now_param(sigbuf,   lb) {
  lb = tolower(sigbuf)
  return (lb ~ /[(,][ \t]*(mut[ \t]+)?now[ \t]*:/) \
      || (lb ~ /[(,][ \t]*(mut[ \t]+)?clock[ \t]*:/) \
      || (lb ~ /[(,][ \t]*(mut[ \t]+)?[a-z0-9_]*_now[ \t]*:/) \
      || (lb ~ /[(,][ \t]*(mut[ \t]+)?now_[a-z0-9_]*[ \t]*:/) \
      || (lb ~ /[(,][ \t]*(mut[ \t]+)?[a-z0-9_]*_clock[ \t]*:/) \
      || (lb ~ /[(,][ \t]*(mut[ \t]+)?clock_[a-z0-9_]*[ \t]*:/)
}
# A clock-reading helper: any function (anywhere in the file -- a test-only
# fixture builder in tests/, or a production constructor; whichever is called
# twice and compared is the one that matters) whose own body binds a `let` to
# an unconditional, direct `now_ns()` or `SystemTime::now()` call, and whose
# signature takes no now/clock parameter. The bare functions themselves are
# not registered here: a test reading the live clock straight, with no helper
# hiding the read, is a deliberate live-clock check (confirming a default
# resolves to real time, say), not the #4000 shape this rule targets.
function find_clock_fns(   i, k, name, sig, depth, started, seg, bi, nb, bc) {
  for (i = 1; i <= nlines; i++) {
    if (!match(clean[i], /(^|[ \t])fn[ \t]+[A-Za-z_][A-Za-z0-9_]*/)) continue
    name = substr(clean[i], RSTART, RLENGTH)
    sub(/^[ \t]*fn[ \t]+/, "", name)
    if (index(clean[i], "(") == 0) continue
    sig = substr(clean[i], index(clean[i], "("))
    k = i
    while (balance(sig) > 0 && k < nlines) { k++; sig = sig " " clean[k] }
    if (fn_sig_has_now_param(sig)) continue
    depth = 0; started = 0
    for (k = i; k <= nlines && k <= i + 2000; k++) {
      seg = clean[k]
      nb = length(seg)
      for (bi = 1; bi <= nb; bi++) {
        bc = substr(seg, bi, 1)
        if (bc == "{") { depth++; started = 1 }
        else if (bc == "}") depth--
      }
      if (started && seg ~ /^[ \t]*let[ \t]+(mut[ \t]+)?[A-Za-z_][A-Za-z0-9_]*[ \t]*=[ \t]*(now_ns\(\)|SystemTime::now\()/) {
        clockfn[name] = 1
      }
      if (started && depth <= 0) break
    }
  }
}
# Within one #[test]/#[tokio::test] function body, tag an identifier bound
# (directly or through a chain of `let`s, in source order -- a test function
# runs top to bottom, so this needs no fixpoint the way Pass A2 does) to a
# a clock-reading helper call. Two differently-tagged identifiers meeting in
# one assertion is two independent clock readings compared: the #4000 shape.
function scan_clock(   i, j, k, cj, cap, name, attr_ok, depth, started, seg, bi, nb, bc,
                        buf, b, lhs, rhs, parts, ntok, tok, ident, call, cnt, tagkey, seen, tagctr) {
  for (i = 1; i <= nlines; i++) {
    if (clean[i] !~ /fn[ \t]+[A-Za-z_][A-Za-z0-9_]*[ \t]*\(/) continue
    attr_ok = 0
    for (j = i - 1; j >= 1; j--) {
      if (raw[j] ~ /^[ \t]*$/) continue
      if (raw[j] ~ /^[ \t]*\/\//) continue
      if (raw[j] ~ /^[ \t]*#\[/) {
        if (raw[j] ~ /#\[[ \t]*(tokio::)?test([ \t]*\(|[ \t]*\])/) attr_ok = 1
        continue
      }
      break
    }
    if (!attr_ok) continue
    depth = 0; started = 0; tagctr = 0
    delete clocktag
    for (k = i; k <= nlines && k <= i + 4000; k++) {
      seg = clean[k]
      nb = length(seg)
      for (bi = 1; bi <= nb; bi++) {
        bc = substr(seg, bi, 1)
        if (bc == "{") { depth++; started = 1 }
        else if (bc == "}") depth--
      }
      if (started && k > i && seg ~ /^[ \t]*let[ \t]/ && index(seg, "=") > 0) {
        lhs = substr(seg, 1, index(seg, "=") - 1)
        rhs = substr(seg, index(seg, "=") + 1)
        if (lhs !~ /[!<>=]$/) {
          gsub(/[^A-Za-z0-9_]+/, " ", lhs)
          ntok = split(lhs, parts, " ")
          ident = ""
          for (tok = 2; tok <= ntok; tok++) {
            if (parts[tok] == "mut") continue
            ident = parts[tok]; break
          }
          if (ident != "") {
            call = ""
            for (name in clockfn) {
              if (is_call(rhs, name)) { call = name; break }
            }
            if (call != "") {
              tagctr++
              clocktag[ident] = "t" tagctr
            } else {
              for (tagkey in clocktag) {
                if (has_word(rhs, tagkey)) { clocktag[ident] = clocktag[tagkey]; break }
              }
            }
          }
        }
      }
      if (started && k > i && seg ~ /assert[a-z_]*!/ && index(seg, "(") > 0) {
        buf = substr(seg, index(seg, "assert"))
        b = balance(buf)
        cap = 0; cj = k
        while (b > 0 && cap < 40 && cj < nlines) {
          cj++; cap++
          buf = buf " " clean[cj]
          b = balance(buf)
        }
        if (b == 0) {
          delete seen
          cnt = 0
          for (tagkey in clocktag) {
            if (has_word(buf, tagkey) && !(clocktag[tagkey] in seen)) {
              seen[clocktag[tagkey]] = 1
              cnt++
            }
          }
          if (cnt >= 2) {
            report("clock-compared", k, "two independently-read clock values compared in one assertion: build both fixtures from one reading, passed in rather than read by the helper")
          }
        }
      }
      if (started && depth <= 0) break
    }
  }
}
# An #[ignore] whose reason names "flaky" but cites no issue (#<digits>). Raw
# lines, not the stripped skeleton: the reason lives inside a string literal
# or a comment, both of which strip_line removes from clean[]. The walk
# upward passes over other attribute lines (`#[test]` can sit between the
# comment and `#[ignore]`, in either order) without stopping, so a reason
# commented above the whole attribute block is read regardless of where
# `#[ignore]` falls within it.
function scan_ignore_flaky(   i, j, combined) {
  for (i = 1; i <= nlines; i++) {
    if (raw[i] !~ /#\[[ \t]*ignore([ \t]*=|[ \t]*\])/) continue
    combined = raw[i]
    for (j = i - 1; j >= 1; j--) {
      if (raw[j] ~ /^[ \t]*\/\//) { combined = combined " " raw[j]; continue }
      if (raw[j] ~ /^[ \t]*#\[/) continue
      break
    }
    if (tolower(combined) !~ /flaky/) continue
    if (combined ~ /#[0-9]+/) continue
    report("ignore-flaky", i, "#[ignore] names flaky but cites no issue: file one, or cite #<the one that already exists>")
  }
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
  # Rule: ignore-flaky. No file-scope state to build first.
  scan_ignore_flaky()
  # Rule: clock-compared. find_clock_fns() must run first: scan_clock() reads
  # the clockfn set it builds.
  find_clock_fns()
  scan_clock()
}
BEGIN { SQ = sprintf("%c", 39) }   # a single quote, unwriteable in this quoting
FNR == 1 {
  if (nlines > 0) scan()
  delete raw; delete clean; delete timing; delete clockfn
  nlines = 0; cfg_test_line = 0
  sstate = 0; shashes = 0; sesc = 0; bdepth = 0
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
scan_status=0
xargs -0 awk "${awk_prog}" <"${sources_file}" >"${findings_file}" || scan_status=$?
if [[ "${scan_status}" -ne 0 ]]; then
  echo "check-test-hygiene.sh: the source scan failed (exit ${scan_status})." >&2
  echo "  Refusing to report a result: an empty findings file after a failed" >&2
  echo "  scan is indistinguishable from a clean tree." >&2
  exit 70
fi

# --- rule 3 ----------------------------------------------------------------
#
# proptest's default persistence writes a caught seed next to the source:
# `<crate>/proptest-regressions/<path under src>.txt` for a property test in
# `src/`, and `<dir>/<name>.proptest-regressions` for one under `tests/`. Both
# forms exist in this repo. A seed that exists but is untracked, or that a
# .gitignore covers, is never replayed by `cargo test`.
while IFS= read -r -d '' src; do
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
persistence_hits="$(mktemp "${TMPDIR:-/tmp}/ravel-test-hygiene-fp.XXXXXX")"
# grep's own status separates the three outcomes that matter here: 0 matched,
# 1 nothing matched, 2 could not read. xargs destroys exactly that distinction,
# because GNU xargs maps every utility exit from 1 to 125 onto a single 123, so
# an unreadable source is indistinguishable from a clean scan. Call grep
# directly, in batches to stay under the argument limit, and read its status.
persistence_sources=()
while IFS= read -r -d '' persistence_src; do
  persistence_sources+=("${persistence_src}")
done <"${sources_file}"

persistence_batch=0
while [[ "${persistence_batch}" -lt "${#persistence_sources[@]}" ]]; do
  persistence_status=0
  grep -Hn 'failure_persistence' \
    "${persistence_sources[@]:${persistence_batch}:500}" \
    >>"${persistence_hits}" || persistence_status=$?
  if [[ "${persistence_status}" -ne 0 && "${persistence_status}" -ne 1 ]]; then
    echo "check-test-hygiene.sh: the failure_persistence scan failed (grep exit ${persistence_status})." >&2
    rm -f "${persistence_hits}"
    exit 70
  fi
  persistence_batch=$(( persistence_batch + 500 ))
done
sed 's/^\([^:]*:[0-9]*\):.*$/\1: proptest-seed: failure_persistence override can disable seed writing; the caught case is then lost/' \
  <"${persistence_hits}" >>"${findings_file}"
rm -f "${persistence_hits}"

count=$(grep -c '' "${findings_file}")
if [[ "${count}" -eq 0 ]]; then
  echo "check-test-hygiene.sh: clean ($(tr -cd '\0' <"${sources_file}" | wc -c | tr -d ' ') Rust sources)"
  exit 0
fi

sort -t: -k1,1 -k2,2n "${findings_file}"
echo "check-test-hygiene.sh: ${count} finding(s)" >&2
exit 1
