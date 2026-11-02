#!/usr/bin/env bash
# kit.test.sh -- self-contained tests for the selective-investigations
# harness (issue #3699). No AWS, no running ravel-server, no network.
# Runs in well under two minutes. Wired into CI's doc-scripts job
# alongside benchmarks/clickbench/kit/kit.test.sh.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

FAILED=0
PASS_COUNT=0

check() {
  # check <description> <command...>
  local desc="$1"; shift
  if "$@"; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "FAIL: $desc" >&2
    FAILED=1
  fi
}

pyeq() {
  # pyeq <description> <python-statements-ending-in-a-bare-boolean-expr>
  # The body may be multiple statements (imports, assignments, loops); only
  # the LAST statement must be a bare expression. It is exec'd as a module
  # minus its last statement, then that last statement is eval'd on its own
  # to get the pass/fail boolean -- this lets every call site read like a
  # small script instead of a single crammed expression.
  local desc="$1" body="$2"
  local bodyfile="$TMP/pyeq_body.py" errfile="$TMP/pyeq_err.log"
  printf '%s\n' "$body" > "$bodyfile"
  python3 - "$bodyfile" <<'PYEOF' 2>"$errfile"
import ast
import sys

path = sys.argv[1]
src = open(path).read()
tree = ast.parse(src)
if not tree.body or not isinstance(tree.body[-1], ast.Expr):
    print("pyeq body must end in a bare boolean expression", file=sys.stderr)
    sys.exit(2)
setup = ast.Module(body=tree.body[:-1], type_ignores=[])
final = ast.Expression(body=tree.body[-1].value)
ns = {}
exec(compile(setup, "<pyeq>", "exec"), ns)
result = eval(compile(final, "<pyeq>", "eval"), ns)
sys.exit(0 if result else 1)
PYEOF
  local rc=$?
  if [ "$rc" = 0 ]; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "FAIL: $desc" >&2
    cat "$errfile" >&2
    FAILED=1
  fi
  rm -f "$bodyfile" "$errfile"
}

echo "== gen_corpus.py correctness =="

CORPUS1="$TMP/corpus1"
python3 "$HERE/gen_corpus.py" --traces 1000 --spans-per-trace 10 --logs-per-trace 3 \
  --hours 2 --seed 7 --out "$CORPUS1" > "$TMP/gen1.log" 2>&1
check "gen_corpus.py (scale 1) exits 0" test "${PIPESTATUS[0]:-$?}" = 0 || true
if [ ! -f "$CORPUS1/truth.json" ]; then
  echo "FAIL: gen_corpus.py did not write truth.json" >&2
  cat "$TMP/gen1.log" >&2
  FAILED=1
else
  PASS_COUNT=$((PASS_COUNT + 1))
fi

T1="$CORPUS1/truth.json"

pyeq "spans_written == traces * spans_per_trace (1000*10)" "
import json
t = json.load(open('$T1'))
t['spans_written'] == 10000
"

pyeq "logs_written == traces * logs_per_trace (1000*3)" "
import json
t = json.load(open('$T1'))
t['logs_written'] == 3000
"

pyeq "failing cell has exactly min(50, traces) planted trace ids" "
import json
t = json.load(open('$T1'))
len(t['failing_cell']['trace_ids']) == 50 and t['failing_cell']['count'] == 50
"

pyeq "request_id.present count == 1, request_id.absent count == 0" "
import json
t = json.load(open('$T1'))
t['request_id']['present']['count'] == 1 and t['request_id']['absent']['count'] == 0
"

pyeq "every tag needle's expected_count is consistent with its wanted block count and BLOCK_ROWS=256 (at most the single last, partial block may fall short of a full 256)" "
import json, math
t = json.load(open('$T1'))
block_rows = t['block_rows']
n_blocks = math.ceil(t['base_logs_count'] / block_rows)
ok = True
for n in t['tag_needles']:
    if n['n_blocks_total'] != n_blocks:
        ok = False
    if n['frequency'] == '1_block':
        want = 1
    elif n['frequency'] == '0.1pct':
        want = max(1, round(0.001 * n_blocks))
    else:
        want = max(1, round(0.10 * n_blocks))
    if not ((want - 1) * block_rows < n['expected_count'] <= want * block_rows):
        ok = False
ok
"

pyeq "logs trace_ids are a subset of spans trace_ids (saved_trace_ids)" "
import json
t = json.load(open('$T1'))
saved = {s['trace_id'] for s in t['saved_trace_ids']}
len(saved) > 0
"

pyeq "all logs.jsonl trace_ids appear among spans.jsonl trace_ids" "
import json
span_ids = set()
with open('$CORPUS1/spans.jsonl') as fh:
    for line in fh:
        span_ids.add(json.loads(line)['trace_id'])
log_ids = set()
with open('$CORPUS1/logs.jsonl') as fh:
    for line in fh:
        log_ids.add(json.loads(line)['trace_id'])
log_ids <= span_ids
"

pyeq "failing cell's root spans are all status=error" "
import json
t = json.load(open('$T1'))
failing = set(t['failing_cell']['trace_ids'])
service = t['failing_cell']['service']
hour = t['failing_cell']['hour']
roots_seen = 0
errors_seen = 0
with open('$CORPUS1/spans.jsonl') as fh:
    for line in fh:
        row = json.loads(line)
        if row['trace_id'] in failing and row.get('parent_span_id') in (None, ''):
            roots_seen += 1
            if row.get('status') == 'error':
                errors_seen += 1
roots_seen == len(failing) and errors_seen == roots_seen
"

echo "== gen_corpus.py scale invariance (scale=10 keeps planted counts identical) =="

CORPUS10="$TMP/corpus10"
python3 "$HERE/gen_corpus.py" --traces 1000 --spans-per-trace 10 --logs-per-trace 3 \
  --hours 2 --seed 7 --scale 10 --out "$CORPUS10" > "$TMP/gen10.log" 2>&1
T10="$CORPUS10/truth.json"
if [ ! -f "$T10" ]; then
  echo "FAIL: scale=10 run did not write truth.json" >&2
  cat "$TMP/gen10.log" >&2
  FAILED=1
else
  PASS_COUNT=$((PASS_COUNT + 1))
fi

pyeq "scale=10: failing_cell, request_id, tag_needle counts are bit-identical to scale=1" "
import json
t1 = json.load(open('$T1'))
t10 = json.load(open('$T10'))
(t1['failing_cell']['count'] == t10['failing_cell']['count']
 and t1['request_id']['present']['count'] == t10['request_id']['present']['count']
 and t1['request_id']['absent']['count'] == t10['request_id']['absent']['count']
 and [n['expected_count'] for n in t1['tag_needles']] == [n['expected_count'] for n in t10['tag_needles']])
"

pyeq "scale=10: logs_written scaled by noise multiplier (greater than scale=1's)" "
import json
t1 = json.load(open('$T1'))
t10 = json.load(open('$T10'))
t10['logs_written'] > t1['logs_written']
"

echo "== evaluate.py: class closure, fixture-based correctness =="

OUT_COMPLETE="$TMP/eval_complete"
python3 "$HERE/evaluate.py" --calls "$HERE/fixtures/calls_complete.jsonl" \
  --truth "$HERE/fixtures/truth_complete.json" --bands "$HERE/bands.toml" \
  --stamps "$HERE/fixtures/stamps_complete" --out "$OUT_COMPLETE" > "$TMP/eval_complete.log" 2>&1
RC=$?
if [ "$RC" = 0 ]; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  echo "FAIL: evaluate.py on the complete fixture must exit 0" >&2
  cat "$TMP/eval_complete.log" >&2
  FAILED=1
fi

pyeq "complete fixture: every bands.toml figure appears in results.json exactly once" "
import json, tomllib
bands = tomllib.load(open('$HERE/bands.toml', 'rb'))
results = json.load(open('$OUT_COMPLETE/results.json'))
counts = {}
for f in results['figures']:
    counts[f['name']] = counts.get(f['name'], 0) + 1
all(counts.get(row['figure'], 0) == 1 for row in bands.values()) and results['errors'] == []
"

OUT_BANDMISS="$TMP/eval_bandmiss"
python3 "$HERE/evaluate.py" --calls "$HERE/fixtures/calls_band_miss.jsonl" \
  --truth "$HERE/fixtures/truth_complete.json" --bands "$HERE/bands.toml" \
  --stamps "$HERE/fixtures/stamps_complete" --out "$OUT_BANDMISS" > "$TMP/eval_bandmiss.log" 2>&1
RC=$?
if [ "$RC" != 0 ]; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  echo "FAIL: evaluate.py on the band-miss fixture must exit nonzero" >&2
  FAILED=1
fi
pyeq "band-miss fixture: row_01 verdicts are MISS" "
import json
results = json.load(open('$OUT_BANDMISS/results.json'))
misses = {v['row'] for v in results['verdicts'] if v['verdict'] == 'MISS'}
'row_01_spans_lookup_no_range_folded_leaves' in misses
"

OUT_WRONG="$TMP/eval_wronganswer"
python3 "$HERE/evaluate.py" --calls "$HERE/fixtures/calls_wrong_answer.jsonl" \
  --truth "$HERE/fixtures/truth_complete.json" --bands "$HERE/bands.toml" \
  --stamps "$HERE/fixtures/stamps_complete" --out "$OUT_WRONG" > "$TMP/eval_wrong.log" 2>&1
RC=$?
if [ "$RC" != 0 ]; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  echo "FAIL: evaluate.py on the wrong-answer fixture must exit nonzero" >&2
  FAILED=1
fi
pyeq "wrong-answer fixture: logs_request_id_present/folded_leaves status is wrong_answer and correctness.pass_fraction < 1.0" "
import json
results = json.load(open('$OUT_WRONG/results.json'))
detail = next(s for s in results['statements'] if s['statement'] == 'logs_request_id_present' and s['state'] == 'folded_leaves')
pf = next(f['value'] for f in results['figures'] if f['name'] == 'correctness.pass_fraction')
detail['status'] == 'wrong_answer' and pf < 1.0
"

echo "== evaluate.py: check_bands() unit tests (missing / duplicate) =="

pyeq "check_bands reports 'missing <figure>' when a bands.toml figure is absent" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
bands = {'row_x': {'figure': 'never.computed', 'target': 'value == 1', 'miss': 'value != 1', 'lanes': ['local'], 'judged': True}}
errors, verdicts = evaluate.check_bands([], bands)
errors == ['missing never.computed'] and verdicts == []
"

pyeq "check_bands reports 'duplicate <figure>' when a figure is emitted twice" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
figures = [{'name': 'dup.figure', 'value': 1.0}, {'name': 'dup.figure', 'value': 99.0}]
bands = {'row_x': {'figure': 'dup.figure', 'target': 'value == 1', 'miss': 'value != 1', 'lanes': ['local'], 'judged': True}}
errors, verdicts = evaluate.check_bands(figures, bands)
errors == ['duplicate dup.figure'] and verdicts == []
"

echo "== evaluate.py: two wrong evaluators the real check_bands() rules out =="
# Both flips are performed against a COPY of evaluate.py so the real,
# tested file on disk is never modified by this test run.

EVALUATE_SHA_BEFORE="$(sha256sum "$HERE/evaluate.py" | awk '{print $1}')"

cp "$HERE/evaluate.py" "$TMP/evaluate_flip1.py"
python3 - "$TMP/evaluate_flip1.py" <<'PYEOF'
import sys
path = sys.argv[1]
src = open(path).read()
# Flip 1 (mirrors evaluate.py lines 168-170): a missing figure silently
# becomes a passing None-valued verdict instead of an error.
old = '''        n = counts.get(fig, 0)
        if n == 0:
            errors.append(f"missing {fig}")
            continue
        if n > 1:
            errors.append(f"duplicate {fig}")
            continue'''
new = '''        n = counts.get(fig, 0)
        if n == 0:
            verdict_rows.append({"row": row_name, "figure": fig, "value": None, "verdict": "PASS"})
            continue
        if n > 1:
            errors.append(f"duplicate {fig}")
            continue'''
assert old in src, "evaluate.py no longer matches the expected check_bands() body at lines 168-170; update this flip"
open(path, "w").write(src.replace(old, new))
PYEOF
pyeq "wrong evaluator 1 (missing-as-pass) wrongly reports no errors and a PASS for a figure never emitted" "
import importlib.util
spec = importlib.util.spec_from_file_location('evaluate_flip1', '$TMP/evaluate_flip1.py')
flip1 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(flip1)
bands = {'row_x': {'figure': 'never.computed', 'target': 'value == 1', 'miss': 'value != 1', 'lanes': ['local'], 'judged': True}}
errors, verdicts = flip1.check_bands([], bands)
errors == [] and verdicts[0]['verdict'] == 'PASS'
"
pyeq "the real check_bands() does NOT make this mistake: same input still reports 'missing never.computed'" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
bands = {'row_x': {'figure': 'never.computed', 'target': 'value == 1', 'miss': 'value != 1', 'lanes': ['local'], 'judged': True}}
errors, verdicts = evaluate.check_bands([], bands)
errors == ['missing never.computed']
"

cp "$HERE/evaluate.py" "$TMP/evaluate_flip2.py"
python3 - "$TMP/evaluate_flip2.py" <<'PYEOF'
import sys
path = sys.argv[1]
src = open(path).read()
# Flip 2 (mirrors evaluate.py lines 171-173): the duplicate check is
# deleted outright. Because the counting loop's values[name] = value
# naturally overwrites on each repeat, this silently keeps whichever
# duplicate was written LAST and reports a single clean verdict for it,
# hiding that the figure was ever duplicated.
old = '''        if n > 1:
            errors.append(f"duplicate {fig}")
            continue
        value = values[fig]'''
new = '''        value = values[fig]'''
assert old in src, "evaluate.py no longer matches the expected check_bands() body at lines 171-173; update this flip"
open(path, "w").write(src.replace(old, new))
PYEOF
pyeq "wrong evaluator 2 (delete duplicate check) silently keeps the LAST duplicate value with no error" "
import importlib.util
spec = importlib.util.spec_from_file_location('evaluate_flip2', '$TMP/evaluate_flip2.py')
flip2 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(flip2)
figures = [{'name': 'dup.figure', 'value': 1.0}, {'name': 'dup.figure', 'value': 99.0}]
bands = {'row_x': {'figure': 'dup.figure', 'target': 'value == 1', 'miss': 'value != 1', 'lanes': ['local'], 'judged': True}}
errors, verdicts = flip2.check_bands(figures, bands)
errors == [] and verdicts[0]['value'] == 99.0 and verdicts[0]['verdict'] == 'MISS'
"
pyeq "the real check_bands() does NOT make this mistake: same input still reports 'duplicate dup.figure'" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
figures = [{'name': 'dup.figure', 'value': 1.0}, {'name': 'dup.figure', 'value': 99.0}]
bands = {'row_x': {'figure': 'dup.figure', 'target': 'value == 1', 'miss': 'value != 1', 'lanes': ['local'], 'judged': True}}
errors, verdicts = evaluate.check_bands(figures, bands)
errors == ['duplicate dup.figure']
"

EVALUATE_SHA_AFTER="$(sha256sum "$HERE/evaluate.py" | awk '{print $1}')"
check "evaluate.py on disk is unmodified by the flip tests above" test "$EVALUATE_SHA_BEFORE" = "$EVALUATE_SHA_AFTER"

echo "== evaluate.py: cost formula hand-checked against pre-computed arithmetic =="

pyeq "query_cost worked example: 1000 requests, 0 cpu_s, get=0.0004/1000 => 0.0004 exactly" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
# requests*get/1000 + cpu_seconds*instance_per_hour/3600
got = evaluate.query_cost(1000, 0.0, 0.0004, 0.68)
expect = 1000 * 0.0004 / 1000.0
abs(got - expect) < 1e-9 and abs(got - 0.0004) < 1e-9
"

pyeq "maintenance_cost_per_part_month worked example matches (requests*get/1000 + GiB*storage) hand-computed" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
got = evaluate.maintenance_cost_per_part_month(
    fold_section_gets=10, leaf_put_requests=40, leaf_bytes=0,
    get_price_per_1000=0.0004, storage_price_per_gb_month=0.023,
)
# requests = fold_section_gets + leaf_put_requests = 50
expect = 50 * 0.0004 / 1000.0
ok1 = abs(got - expect) < 1e-9 and abs(got - 0.00002) < 1e-9
got2 = evaluate.maintenance_cost_per_part_month(
    fold_section_gets=10, leaf_put_requests=40, leaf_bytes=1_000_000_000,
    get_price_per_1000=0.0004, storage_price_per_gb_month=0.023,
)
expect2 = 50 * 0.0004 / 1000.0 + (1_000_000_000 / 1024.0**3) * 0.023
ok2 = abs(got2 - expect2) < 1e-9 and abs(got2 - 0.021440419216156005) < 1e-9
ok1 and ok2
"

pyeq "breakeven_lookups_per_part_month(maintenance_cost, savings_per_lookup) == maintenance_cost / savings_per_lookup (hand-computed)" "
import sys
sys.path.insert(0, '$HERE')
import evaluate
got = evaluate.breakeven_lookups_per_part_month(0.021440419216156005, 0.0001)
abs(got - 214.40419216156006) < 1e-6
"

pyeq "breakeven_lookups_per_part_month returns inf when savings_per_lookup <= 0" "
import sys, math
sys.path.insert(0, '$HERE')
import evaluate
math.isinf(evaluate.breakeven_lookups_per_part_month(1.0, 0.0))
"

echo "== box/run.sh --dry-run: step ordering and stamps around every state transition =="

cat > "$TMP/run.local.toml" <<'EOF'
sha = "deadbeef"
lane = "local"
states = "unfolded,folded_no_leaves,folded_leaves"
control = false

[corpus]
traces = 1000
hours = 2
logs_per_trace = 3
seed = 7
scale = 1
EOF

DRY_OUT="$TMP/dry_out"
RUN_OUT="$DRY_OUT" bash "$HERE/box/run.sh" "$TMP/run.local.toml" --dry-run > "$TMP/dry.log" 2>&1
RC=$?
if [ "$RC" = 0 ]; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  echo "FAIL: run.sh --dry-run (local lane) must exit 0" >&2
  cat "$TMP/dry.log" >&2
  FAILED=1
fi

pyeq "dry-run step order: bootstrap, then store, then corpus, then declare, then load, before any state stamp" "
# run()'s dry-run printer %q-escapes every argument, so a multi-word
# command like 'load --signal spans' shows up with its spaces backslash-
# escaped ('load\\\\ --signal\\\\ spans'); search for the escaped form.
log = open('$TMP/dry.log').read()
i_cargo = log.find('cargo build')
i_corpus = log.find('gen_corpus.py')
i_declare = log.find('typed-attr-column')
i_load = log.find('load\\\\ --signal\\\\ spans')
i_stamp = log.find('stamps/unfolded.before.json')
-1 not in (i_cargo, i_corpus, i_declare, i_load, i_stamp) and i_cargo < i_corpus < i_declare < i_load < i_stamp
"

pyeq "dry-run: every requested state gets a before AND an after stamp, in order" "
log = open('$TMP/dry.log').read()
states = ['unfolded', 'folded_no_leaves', 'folded_leaves']
positions = []
for s in states:
    b = log.find(f'stamps/{s}.before.json')
    a = log.find(f'stamps/{s}.after.json')
    assert b != -1 and a != -1, f'missing stamp for {s}'
    assert b < a, f'{s}: before stamp must precede after stamp'
    positions.append(b)
positions == sorted(positions)
"

pyeq "dry-run: statements only run between a state's before and after stamp" "
log = open('$TMP/dry.log').read().splitlines()
in_state = False
state_name = None
ok = True
for line in log:
    for s in ['unfolded', 'folded_no_leaves', 'folded_leaves']:
        if f'stamps/{s}.before.json' in line:
            in_state = True
            state_name = s
        if f'stamps/{s}.after.json' in line:
            in_state = False
    if 'run_query' in line and not in_state:
        ok = False
ok
"

echo "== box/run.sh --dry-run: s3 lane upload + DONE marker + shutdown =="

cat > "$TMP/run.s3.toml" <<'EOF'
sha = "deadbeef"
lane = "s3"
bucket = "inv-bucket"
region = "us-east-1"
prefix = "inv"
run_id = "run-0001"
states = "unfolded,many_parts"
control = true

[corpus]
traces = 1000
hours = 2
logs_per_trace = 3
seed = 7
scale = 1
EOF
S3_DRY_OUT="$TMP/dry_s3_out"
RUN_OUT="$S3_DRY_OUT" bash "$HERE/box/run.sh" "$TMP/run.s3.toml" --dry-run > "$TMP/dry_s3.log" 2>&1
RC=$?
if [ "$RC" = 0 ]; then
  PASS_COUNT=$((PASS_COUNT + 1))
else
  echo "FAIL: run.sh --dry-run (s3 lane) must exit 0" >&2
  cat "$TMP/dry_s3.log" >&2
  FAILED=1
fi
pyeq "s3-lane dry-run: many_parts is reported unsupported-on-this-build, not run as a real state" "
# %q-escapes every space in the printed echo args, so check the two words
# separately rather than as one phrase with literal spaces.
log = open('$TMP/dry_s3.log').read()
'many_parts:' in log and 'unsupported-on-this-build' in log and 'stamps/many_parts.before.json' not in log
"
pyeq "s3-lane dry-run: upload, DONE marker and shutdown happen after every state, in order" "
log = open('$TMP/dry_s3.log').read()
i_after = log.find('stamps/unfolded.after.json')
i_upload = log.find('aws s3 cp --recursive')
i_done = log.find('/DONE')
i_shutdown = log.find('shutdown -h now')
-1 not in (i_after, i_upload, i_done, i_shutdown) and i_after < i_upload < i_done < i_shutdown
"

bash -n "$HERE/box/run.sh" && PASS_COUNT=$((PASS_COUNT + 1)) || { echo "FAIL: box/run.sh has a syntax error" >&2; FAILED=1; }
bash -n "$HERE/box/launch.sh" && PASS_COUNT=$((PASS_COUNT + 1)) || { echo "FAIL: box/launch.sh has a syntax error" >&2; FAILED=1; }
bash -n "$HERE/box/fetch.sh" && PASS_COUNT=$((PASS_COUNT + 1)) || { echo "FAIL: box/fetch.sh has a syntax error" >&2; FAILED=1; }

echo "== statements.toml validity =="

pyeq "statements.toml parses and every id is unique" "
import tomllib
doc = tomllib.load(open('$HERE/statements.toml', 'rb'))
ids = [s['id'] for s in doc['statement']]
len(ids) == len(set(ids)) and len(ids) > 0
"

pyeq "every statement declares id/table/sql/states/truth_key/cold_only/supported/acceptance" "
import tomllib
doc = tomllib.load(open('$HERE/statements.toml', 'rb'))
required = {'id', 'table', 'sql', 'states', 'truth_key', 'cold_only', 'supported', 'acceptance'}
all(required <= set(s.keys()) for s in doc['statement'])
"

pyeq "every supported statement's truth_key ('none' excepted) resolves against truth.json's schema" "
import tomllib, json
doc = tomllib.load(open('$HERE/statements.toml', 'rb'))
truth = json.load(open('$T1'))
def resolves(key):
    if key == 'none':
        return True
    val = truth
    for part in key.split('.'):
        if isinstance(val, dict) and part in val:
            val = val[part]
        elif isinstance(val, list):
            # tag_needles is a list of {'value': ..., ...} dicts; a dotted
            # path segment after it names one entry's 'value'.
            match = [item for item in val if isinstance(item, dict) and item.get('value') == part]
            if not match:
                return False
            val = match[0]
        else:
            return False
    return True
bad = [s['id'] for s in doc['statement'] if s['supported'] and not resolves(s['truth_key'])]
bad == []
"

pyeq "every statement's acceptance row(s) exist in bands.toml's Acceptance numbering (1-17)" "
import tomllib
doc = tomllib.load(open('$HERE/statements.toml', 'rb'))
bad = [s['id'] for s in doc['statement'] for row in s['acceptance'] if not (1 <= row <= 17)]
bad == []
"

echo "== bands.toml validity =="

pyeq "bands.toml parses, covers all 17 Acceptance-table rows (row_01 carries 2 entries: p50 and p99), and every entry has figure/target/miss/lanes/judged" "
import tomllib
doc = tomllib.load(open('$HERE/bands.toml', 'rb'))
required = {'figure', 'target', 'miss', 'lanes', 'judged'}
row_numbers = {name.split('_')[1] for name in doc}
len(doc) == 18 and len(row_numbers) == 17 and all(required <= set(row.keys()) for row in doc.values())
"

echo
echo "pass_count=$PASS_COUNT failed=$FAILED"
if [ "$FAILED" != 0 ]; then
  echo "kit.test.sh: FAILED"
  exit 1
fi
echo "kit.test.sh: PASSED ($PASS_COUNT checks)"
