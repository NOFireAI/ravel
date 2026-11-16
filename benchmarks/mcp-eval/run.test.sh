#!/usr/bin/env bash
# Cases for run.sh (issue #1385). Offline: no server, no agent.
#
#   a  the checked-in corpus                -> --check exit 0, PASS
#   b  a case with no expected file         -> exit 1, "case B2: no expected file"
#   c  a journey with fewer than 3 cases    -> exit 1, "journey E has 2 cases"
#   d  two cases sharing an id              -> exit 1, "duplicate case id A1"
#   e  a figure printed twice               -> exit 1, "printed 2 times"
#   f  a figure not printed                 -> exit 1, "figure fixture.spans missing"
#   g  --run without MCP_EVAL_AGENT_CMD     -> exit 64, names the variable,
#                                              writes no results
#   h  --run --out inside the repository    -> exit 64, writes nothing there
#   i  --run with a stub agent, one case    -> exit 0, record.json scored
#                                              (correct, call count, store
#                                              requests and wire bytes from
#                                              budget.actual), summary.tsv
#
# Then each case is run against a mutant of run.sh or lib/evaltool.py that
# lacks the check the case exists for, and the case must fail there. A case
# that passes against its mutant proves nothing, so that is a failure here.
#
# MCP_EVAL_RUN_SH overrides the run.sh under test.
#
# Run: bash benchmarks/mcp-eval/run.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd -P)"
RUN_SH="${MCP_EVAL_RUN_SH:-$HERE/run.sh}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/mcp-eval-test.XXXXXX")" || exit 70
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0
ok() {
  pass=$((pass + 1))
  echo "ok   $1"
}
bad() {
  fail=$((fail + 1))
  echo "FAIL $1" >&2
}

# corpus_copy NAME -> prints a fresh copy of the corpus tree
corpus_copy() {
  d="$TMP/corpus-$1"
  rm -rf "$d"
  mkdir -p "$d"
  cp -R "$HERE/bands.json" "$HERE/corpus" "$HERE/expected" "$HERE/fixture" "$d/"
  echo "$d"
}

# expect NAME WANT_EXIT GOT_EXIT LOG NEEDLE
expect() {
  if [ "$3" -ne "$2" ]; then
    echo "  $1: exit $3, want $2; output:" >&2
    sed 's/^/    /' "$4" >&2
    return 1
  fi
  if [ -n "$5" ] && ! grep -qF -- "$5" "$4"; then
    echo "  $1: output lacks '$5'; output:" >&2
    sed 's/^/    /' "$4" >&2
    return 1
  fi
  return 0
}

case_a() {
  code=0
  bash "$1" --check >"$TMP/a.log" 2>&1 || code=$?
  expect a 0 "$code" "$TMP/a.log" "run.sh --check: PASS"
}

case_b() {
  d=$(corpus_copy b)
  rm "$d/expected/B2.json"
  code=0
  MCP_EVAL_DIR=$d bash "$1" --check >"$TMP/b.log" 2>&1 || code=$?
  expect b 1 "$code" "$TMP/b.log" "case B2: no expected file expected/B2.json"
}

case_c() {
  d=$(corpus_copy c)
  rm "$d/corpus/E3.json" "$d/expected/E3.json"
  code=0
  MCP_EVAL_DIR=$d bash "$1" --check >"$TMP/c.log" 2>&1 || code=$?
  expect c 1 "$code" "$TMP/c.log" "journey E has 2 cases; at least 3 required"
}

case_d() {
  d=$(corpus_copy d)
  cp "$d/corpus/A1.json" "$d/corpus/A0.json"
  code=0
  MCP_EVAL_DIR=$d bash "$1" --check >"$TMP/d.log" 2>&1 || code=$?
  expect d 1 "$code" "$TMP/d.log" "duplicate case id A1 in"
}

# good_figures OUT -> the figure lines of a passing --check
good_figures() {
  code=0
  bash "$HERE/run.sh" --check >"$TMP/figures.log" 2>&1 || code=$?
  [ "$code" -eq 0 ] || return 1
  grep '^figure ' "$TMP/figures.log" >"$1" || return 1
}

case_e() {
  good_figures "$TMP/e.fig" || { echo "  e: the checked-in corpus does not pass" >&2; return 1; }
  grep '^figure fixture.spans=' "$TMP/e.fig" >"$TMP/e.dup"
  cat "$TMP/e.dup" >>"$TMP/e.fig"
  code=0
  bash "$1" --verify-figures "$TMP/e.fig" >"$TMP/e.log" 2>&1 || code=$?
  expect e 1 "$code" "$TMP/e.log" "figure fixture.spans printed 2 times; want exactly once"
}

case_f() {
  good_figures "$TMP/f.all" || { echo "  f: the checked-in corpus does not pass" >&2; return 1; }
  grep -v '^figure fixture.spans=' "$TMP/f.all" >"$TMP/f.fig"
  code=0
  bash "$1" --verify-figures "$TMP/f.fig" >"$TMP/f.log" 2>&1 || code=$?
  expect f 1 "$code" "$TMP/f.log" "figure fixture.spans missing; want 480"
}

# run_env OUT -> an --out directory that holds an anchor, as --fixture leaves it
run_out() {
  rm -rf "$1"
  mkdir -p "$1"
  echo '{"t0_ns": 1700000000000000000}' >"$1/anchor.json"
}

case_g() {
  out="$TMP/g-out"
  run_out "$out"
  code=0
  env -u MCP_EVAL_AGENT_CMD MCP_EVAL_RAVEL_SHA=abc1234 MCP_EVAL_TOKEN=t \
    bash "$1" --run full http://127.0.0.1:9 --out "$out" --runs 1 >"$TMP/g.log" 2>&1 || code=$?
  expect g 64 "$code" "$TMP/g.log" "MCP_EVAL_AGENT_CMD is not set" || return 1
  if [ -e "$out/results" ] || [ -e "$out/prime" ]; then
    echo "  g: results were written without an agent command" >&2
    return 1
  fi
}

write_stub_agent() {
  cat >"$TMP/agent.sh" <<'EOF'
#!/usr/bin/env bash
[ $# -eq 3 ] && [ -s "$1" ] && [ -s "$2" ] || exit 2
[ -n "$MCP_EVAL_URL" ] && [ -n "$MCP_EVAL_PROFILE" ] && [ -n "$MCP_EVAL_TOKEN" ] || exit 3
cat >"$3" <<'J'
{"model": "stub", "model_version": "1",
 "answer": "Over the time range read, metrics hold 3840 samples, logs hold 725 rows, traces hold 480 spans.",
 "completed": true,
 "calls": [
  {"tool": "ravel_describe_data", "arguments": {},
   "response": {"status": "ok", "budget": {"actual": {"store_requests": 7, "bytes_scanned": 4096}}},
   "response_bytes": 900},
  {"tool": "ravel_capabilities", "arguments": {},
   "response": {"status": "ok"}, "response_bytes": 300}
 ]}
J
EOF
  chmod +x "$TMP/agent.sh"
}

case_h() {
  write_stub_agent
  out="$HERE/.run-test-out-$$"
  code=0
  MCP_EVAL_AGENT_CMD=$TMP/agent.sh MCP_EVAL_RAVEL_SHA=abc1234 MCP_EVAL_TOKEN=t \
    bash "$1" --run full http://127.0.0.1:9 --out "$out" --runs 1 >"$TMP/h.log" 2>&1 || code=$?
  created=0
  if [ -e "$out" ]; then created=1; fi
  rm -rf "$out"
  expect h 64 "$code" "$TMP/h.log" "is inside the repository" || return 1
  [ "$created" -eq 0 ] || { echo "  h: a refused --out was still created inside the repository" >&2; return 1; }
}

# A failure repaired by a later call that answers ok_page counts as repaired.
case_j() {
  python3 -I - "$(dirname "$1")/lib" <<'EOF'
import sys
sys.path.insert(0, sys.argv[1])
import evaltool
calls = [{"tool": "ravel_query_sql"}, {"tool": "ravel_query_sql"}]
envs = [{"status": "error", "failure": {"class": "validation"}}, {"status": "ok_page"}]
got = evaltool.retries(calls, envs)
if got != {"validation": 1}:
    sys.stderr.write("  j: retries over an ok_page repair: got %r, want {'validation': 1}\n" % got)
    sys.exit(1)
EOF
}

# field exits non-zero on an absent key; field-opt prints nothing and exits 0.
case_k() {
  printf '{"visibility": {}}\n' >"$TMP/k.json"
  tool="$(dirname "$1")/lib/evaltool.py"
  code=0
  python3 -I "$tool" field "$TMP/k.json" visibility.snapshot_id >"$TMP/k.out" 2>"$TMP/k.err" || code=$?
  [ "$code" -eq 1 ] || { echo "  k: field on an absent key exited $code, want 1" >&2; return 1; }
  code=0
  python3 -I "$tool" field-opt "$TMP/k.json" visibility.snapshot_id >"$TMP/k.out" 2>"$TMP/k.err" || code=$?
  [ "$code" -eq 0 ] && [ ! -s "$TMP/k.out" ] || { echo "  k: field-opt on an absent key exited $code" >&2; return 1; }
}

case_i() {
  write_stub_agent
  d=$(corpus_copy i)
  for f in "$d"/corpus/*.json; do
    [ "$(basename "$f")" = "A1.json" ] || rm "$f"
  done
  out="$TMP/i-out"
  run_out "$out"
  code=0
  MCP_EVAL_DIR=$d MCP_EVAL_AGENT_CMD=$TMP/agent.sh MCP_EVAL_RAVEL_SHA=abc1234 MCP_EVAL_TOKEN=t \
    bash "$1" --run full http://127.0.0.1:9 --out "$out" --runs 1 --condition warm >"$TMP/i.log" 2>&1 || code=$?
  expect i 0 "$code" "$TMP/i.log" "" || return 1
  rec="$out/results/full/warm/run-1/A1/record.json"
  [ -s "$rec" ] || { echo "  i: no $rec" >&2; return 1; }
  [ -s "$out/results/full/warm/summary.tsv" ] || { echo "  i: no summary.tsv" >&2; return 1; }
  python3 -I - "$rec" <<'EOF' || return 1
import json, sys
r = json.load(open(sys.argv[1]))
want = {"case": "A1", "run": 1, "profile": "full", "condition": "warm",
        "ravel_sha": "abc1234", "model": "stub", "model_version": "1",
        "correct": True, "completed": True, "facts_matched": 6, "facts_total": 6,
        "evidence_completeness": 1.0, "tool_calls": 2, "within_call_bound": True,
        "invalid_queries": 0, "store_requests": 7, "store_wire_bytes": 4096,
        "calls_without_actual": 1, "response_bytes": 1200, "agent_exit": 0}
bad = {k: (r.get(k), v) for k, v in want.items() if r.get(k) != v}
if bad:
    sys.stderr.write("  i: record fields (got, want): %r\n" % bad)
    sys.exit(1)
for k in ("store_source", "response_bytes_source", "wall_time_ms"):
    if k not in r:
        sys.stderr.write("  i: record lacks %s\n" % k)
        sys.exit(1)
EOF
}

CASES="a b c d e f g h i j k"
for c in $CASES; do
  if "case_$c" "$RUN_SH"; then ok "case $c"; else bad "case $c"; fi
done

# mutant NAME FILE FROM TO -> prints the run.sh of a tree copy with one edit
mutant() {
  m="$TMP/mutant-$1"
  rm -rf "$m"
  mkdir -p "$m"
  cp -R "$HERE/run.sh" "$HERE/lib" "$m/"
  python3 -I - "$m/$2" "$3" "$4" <<'EOF' || return 1
import sys
path, old, new = sys.argv[1:]
text = open(path).read()
if text.count(old) != 1:
    sys.stderr.write("mutant anchor found %d times in %s: %r\n" % (text.count(old), path, old))
    sys.exit(1)
open(path, "w").write(text.replace(old, new))
EOF
  echo "$m/run.sh"
}

# Each row: case|file|text to replace|replacement. MCP_EVAL_DIR is unset for
# case a, so the mutant tree reads the real corpus through it.
while IFS='|' read -r c file from to; do
  [ -n "$c" ] || continue
  if ! m=$(mutant "$c" "$file" "$from" "$to"); then
    bad "mutant for case $c could not be built"
    continue
  fi
  if MCP_EVAL_DIR=$HERE "case_$c" "$m" >/dev/null 2>&1; then
    bad "case $c passes against its mutant ($file: $from)"
  else
    ok "case $c fails against its mutant"
  fi
done <<'MUTANTS'
a|lib/evaltool.py|if per_journey[j] < 3:|if per_journey[j] < 5:
b|lib/evaltool.py|        if cid not in expected_ids:|        if False:
c|lib/evaltool.py|if per_journey[j] < 3:|if per_journey[j] < 2:
d|lib/evaltool.py|        if cid in seen_at:|        if False:
e|lib/evaltool.py|elif len(got) > 1:|elif False:
f|lib/evaltool.py|        if not got:|        if not got and False:
g|run.sh|[ -n "${MCP_EVAL_AGENT_CMD:-}" ]|true
h|run.sh|      "$repo"/*) die 64|      "$repo"/no-such-dir/*) die 64
i|lib/evaltool.py|"bytes_scanned"|"bytes_read"
j|lib/evaltool.py|("ok", "ok_bounded", "ok_page")|("ok", "ok_bounded")
k|lib/evaltool.py|        if required:|        if False:
MUTANTS

echo "run.test.sh: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
