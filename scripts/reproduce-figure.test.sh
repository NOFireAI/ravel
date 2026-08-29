#!/usr/bin/env bash
# Cases for reproduce-figure.sh. Add a case here before changing a behaviour,
# the way .claude/guards/pretooluse.test.sh works for the hook.
#
# The fixture is the shape that motivated the script: a run measuring N
# statements, one of which failed and carries no timings, and a headline quoting
# a total over N-1.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/reproduce-figure.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
check() { # check <name> <expected-exit> <expected-substring> <args...>
  local name="$1" want_rc="$2" want_sub="$3"; shift 3
  local out rc=0
  out="$("$SCRIPT" "$@" 2>&1)" || rc=$?
  if [ "$rc" != "$want_rc" ]; then
    echo "FAIL $name: exit $rc, wanted $want_rc"; echo "$out" | sed 's/^/    /'; fails=$((fails+1)); return
  fi
  case "$out" in
    *"$want_sub"*) echo "ok   $name" ;;
    *) echo "FAIL $name: output missing '$want_sub'"; echo "$out" | sed 's/^/    /'; fails=$((fails+1)) ;;
  esac
}

# JSON-LINES on purpose: a single json.load() over this file raises "Extra data",
# which is the parsing bug this repo keeps rediscovering.
cat > "$TMP/report.json" <<'JSON'
{"id":"qA_alpha","min_ms":1000.0,"cold_ms":5000.0}
{"id":"qB_beta","min_ms":2000.0,"cold_ms":6000.0}
{"id":"qC_gamma","min_ms":3000.0,"cold_ms":7000.0}
{"id":"qD_failed","error":"pool exhausted"}
JSON

# Full set is 6.00 s of min_ms; dropping qB leaves 4.00 s.
check "exact on a single drop" 0 "EXACT: drop qB_beta" "$TMP/report.json" min_ms 4.00 2
check "exact on the full set"  0 "the claimed total is the FULL set" "$TMP/report.json" min_ms 6.00
check "no basis reproduces it" 1 "No single-statement basis reproduces" "$TMP/report.json" min_ms 4.44
check "unreachable total still refuses to guess" 1 "QUOTE THIS OUTPUT" "$TMP/report.json" cold_ms 99.00

# A failed statement must be excluded from the metric, never counted as zero:
# counting it as zero would reproduce a lower total for the wrong reason, which
# is precisely the class of mistake this script exists to prevent.
check "failed statement excluded, not zeroed" 0 "missing on ['qD_failed']" "$TMP/report.json" min_ms 6.00

# The node count and the metric count are reported separately, because a
# statement present-but-untimed is not the same as a statement absent, and
# conflating them produced a wrong claim in the incident that motivated this.
check "counts nodes and metrics separately" 0 "statements    : 4" "$TMP/report.json" min_ms 6.00

# auto tries every numeric *_ms key, so a caller who does not know which metric
# the headline used still gets an answer.
check "auto finds the cold_ms basis" 0 "EXACT: drop qB_beta" "$TMP/report.json" auto 12.00

# A headline count that disagrees with the surviving set is flagged rather than
# silently accepted: reproducing the number on the wrong-sized basis is a
# near-miss dressed as a hit. This applies to the FULL-SET branch too, which is
# the easier one to forget -- a full-set hit on a wrong-sized basis otherwise
# prints "No subset rule needed" with nothing flagged.
check "count mismatch is flagged on a drop"     0 "WARNING: yields 2 statements, headline claims 7" "$TMP/report.json" min_ms 4.00 7
check "count mismatch is flagged on full set"   0 "WARNING: yields 3 statements, headline claims 7" "$TMP/report.json" min_ms 6.00 7

# A headline claiming MORE statements than the report holds is a different
# situation from a subset, and saying "the basis drops -3" is nonsense.
check "headline larger than report reads sensibly" 0 "cannot be a subset of this report" "$TMP/report.json" min_ms 6.00 7

check "unreadable report exits 2" 2 "cannot read" "$TMP/nonexistent.json" min_ms 1.0

# --- ABORT paths (exit 3) -------------------------------------------------
# These matter more than they look. Exit 1 means "no basis reproduces the
# figure", which is quotable in a filing as evidence a number is wrong. Every
# malformed-question path must therefore be exit 3 and must SAY it is not a
# negative result. A typo in the field name producing a quotable exit 1 would be
# this script committing the exact mistake it exists to prevent.
check "misspelled field aborts, not exit 1" 3 "is not a numeric metric on any statement" "$TMP/report.json" min_msec 4.00
check "misspelled field names alternatives"  3 "available: cold_ms, min_ms"              "$TMP/report.json" min_msec 4.00
check "abort says it is not a negative result" 3 "Do not quote it"                       "$TMP/report.json" min_msec 4.00
check "non-numeric claimed aborts"          3 "is not a number"                          "$TMP/report.json" min_ms notanumber
check "non-integer count aborts"            3 "is not an integer"                        "$TMP/report.json" min_ms 4.00 many

# All-narration input: no JSON at all. Aborts, and says how much text it skipped.
printf 'not json at all\n' > "$TMP/garbage.json"
check "narration-only report aborts"  3 "no parseable JSON documents" "$TMP/garbage.json" min_ms 1.0
check "narration bytes are reported"  3 "bytes of non-JSON text were skipped" "$TMP/garbage.json" min_ms 1.0

# Narration AFTER the JSON is the real report format (a human-readable summary
# follows the document), so it must parse, not abort. Getting this wrong made
# the script refuse the very report it was built from.
printf '{"id":"qA","min_ms":1000.0}\n{"id":"qB","min_ms":3000.0}\nsql_latency_bench report\n  backend : s3\n' > "$TMP/trailing.json"
check "trailing narration still parses" 0 "EXACT: drop qB" "$TMP/trailing.json" min_ms 1.00

# A truncated tail must abort rather than silently summing a subset and printing
# a residual as if the report were complete -- that would manufacture the false
# "unreconstructable" verdict this whole script exists to prevent.
printf '{"id":"qA","min_ms":1000.0}\n{"id":"qB","min_' > "$TMP/truncated.json"
check "truncated tail aborts" 3 "malformed JSON" "$TMP/truncated.json" min_ms 1.00

printf '{"nothing":"here"}\n' > "$TMP/nostmts.json"
check "no statement nodes aborts" 3 "no statement nodes found" "$TMP/nostmts.json" min_ms 1.0

printf '{"id":"qA","note":"no numeric metrics"}\n' > "$TMP/nometrics.json"
check "auto with no numeric metrics aborts" 3 "has nothing to sum" "$TMP/nometrics.json" auto 1.0

# Concatenated documents on one line: the usage text promises this form parses,
# so it must, rather than being absorbed into the buffer and dropped.
printf '{"id":"qA","min_ms":1000.0}{"id":"qB","min_ms":3000.0}\n' > "$TMP/concat.json"
check "concatenated docs on one line parse" 0 "EXACT: drop qB" "$TMP/concat.json" min_ms 1.00

if [ "$fails" -ne 0 ]; then echo; echo "$fails case(s) failed"; exit 1; fi
echo; echo "all reproduce-figure cases passed"
