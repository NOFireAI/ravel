#!/usr/bin/env bash
# Reproduce a published total from the bench report it came from.
#
# Exists because a published headline was disputed as "unreconstructable" when
# it was reconstructable: the run measured 42 timed statements and the headline
# quoted 41, and dropping one statement reproduced the published cold total to
# the cent. The reconstruction takes seconds; the wrong dispute cost a filing, a
# correction, and a planning target that moved the wrong way twice.
#
# So: run this BEFORE claiming a figure is wrong, and QUOTE ITS OUTPUT in
# whatever you file. The quote is the point. An omitted quote is visible on the
# page; an omitted mental step is not.
#
# It answers one question: under what subset of the report's statements does the
# claimed total hold? It searches the full set and every single-statement drop,
# for each available per-statement metric, and prints the residual for each so a
# near-miss is distinguishable from a hit.
#
# It does NOT decide whether a basis is legitimate. A subset that reproduces the
# number tells you the number is arithmetically sound on THAT basis; whether the
# basis is the right one to publish is a judgement, and the answer usually lives
# in the publishing document. Hence the reminder printed at the end: grep the
# publishing document for its own stated basis before concluding anything.
#
# EXIT CODES ARE LOAD-BEARING, because this script's output is meant to be
# quoted as evidence:
#   0  a basis reproducing the figure was found
#   1  no single-statement basis reproduces it -- a real negative result
#   2  the report could not be read
#   3  ABORT: the question was malformed (bad field, bad claimed value,
#      unparseable or empty report). NEVER conflated with 1: an exit 1 can be
#      quoted in a filing as evidence a figure is wrong, so a typo in the field
#      name must not be able to produce it. That failure would be this script
#      committing the exact mistake it exists to prevent.
#
# Usage:
#   scripts/reproduce-figure.sh <report.json> <field> <claimed-total-seconds> [expected-count]
#
#   report.json  bench report, JSON-LINES (one document per line, or a stream of
#                concatenated documents -- both are parsed)
#   field        per-statement key to sum: min_ms, cold_ms, median_ms, max_ms,
#                or `auto` to try every numeric *_ms key present
#   claimed      the published total, in SECONDS
#   count        optional: the statement count the headline claims, which is
#                usually the strongest hint at the subset rule
#
# Example (the case this was built from):
#   scripts/reproduce-figure.sh bench-v23-runs3.json cold_ms 310.97 41
#     -> EXACT: drop q29_referer_length_by_host  (42 -> 41 statements)
set -uo pipefail

REPORT="${1:?usage: reproduce-figure.sh <report.json> <field|auto> <claimed-seconds> [expected-count]}"
FIELD="${2:?usage: reproduce-figure.sh <report.json> <field|auto> <claimed-seconds> [expected-count]}"
CLAIMED="${3:?usage: reproduce-figure.sh <report.json> <field|auto> <claimed-seconds> [expected-count]}"
EXPECT_N="${4:-}"

[ -r "$REPORT" ] || { echo "reproduce-figure: cannot read $REPORT" >&2; exit 2; }

REPORT="$REPORT" FIELD="$FIELD" CLAIMED="$CLAIMED" EXPECT_N="$EXPECT_N" python3 - <<'PY'
import json, math, os, sys

ABORT = 3


def abort(msg, *extra):
    print(f"ABORT: {msg}")
    for line in extra:
        print(f"  {line}")
    print()
    print("This is a malformed question, NOT a negative result. Do not quote it")
    print("as evidence that a figure is wrong.")
    sys.exit(ABORT)


report = os.environ["REPORT"]
field = os.environ["FIELD"]

raw_claimed = os.environ["CLAIMED"]
try:
    claimed = float(raw_claimed)
except ValueError:
    abort(f"claimed total {raw_claimed!r} is not a number.",
          "Pass the published total in SECONDS, e.g. 310.97")
if not math.isfinite(claimed):
    # nan compares False against everything, so a nan claim would sail past the
    # matching loop and exit 1 -- "no basis reproduces the figure", the one
    # output meant to be quoted as evidence. Same trap as a misspelled field.
    abort(f"claimed total {raw_claimed!r} is not finite.",
          "A non-finite claim matches nothing and would read as a real negative result.")

raw_expect = (os.environ.get("EXPECT_N") or "").strip()
expect_n = None
if raw_expect:
    try:
        expect_n = int(raw_expect)
    except ValueError:
        abort(f"expected-count {raw_expect!r} is not an integer.")
    if expect_n < 0:
        abort(f"expected-count {expect_n} is negative.")

# Bench output is JSON-LINES. json.load() over the whole file raises "Extra
# data" on the second document, which has cost real time in this repo. Use
# raw_decode so concatenated documents on one line parse too (the usage text
# above promises that form), and account for every byte: text that never parses
# must be REPORTED, never silently dropped. Silently dropping a truncated tail
# would sum a subset and print a residual as if the report were complete --
# manufacturing exactly the false "unreconstructable" verdict this script
# exists to prevent.
decoder = json.JSONDecoder()
text = open(report, errors="replace").read()
docs, narration_bytes, i, n = [], 0, 0, len(text)
while i < n:
    while i < n and text[i] in " \t\r\n":
        i += 1
    if i >= n:
        break
    if text[i] not in "{[":
        # Narration. These reports carry a human-readable summary alongside the
        # JSON, so non-JSON text is expected -- but it is COUNTED and reported
        # rather than silently dropped.
        nxt = min((p for p in (text.find("{", i), text.find("[", i)) if p != -1),
                  default=-1)
        stop = n if nxt == -1 else nxt
        narration_bytes += stop - i
        i = stop
        continue
    try:
        doc, end = decoder.raw_decode(text, i)
    except ValueError:
        # This region STARTS like a JSON value and does not decode, so it is
        # truncation or corruption, not narration. Summing what came before it
        # would print a residual as if the report were complete -- which is the
        # false "unreconstructable" verdict this script exists to prevent.
        abort(f"malformed JSON in {report} at byte offset {i}.",
              f"first 80 chars: {text[i:i+80]!r}",
              "A truncated or corrupt report would otherwise sum a subset and",
              "print a residual as if it were complete.")
    docs.append(doc)
    i = end

if not docs:
    abort(f"no parseable JSON documents in {report}.",
          f"{narration_bytes} bytes of non-JSON text were skipped.")


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


rows = {}
for d in docs:
    for node in walk(d):
        sid = node.get("id")
        if isinstance(sid, str) and sid[:1] == "q":
            rows[sid] = node

if not rows:
    abort("no statement nodes found",
          "(expected objects with a string `id` beginning 'q').")

# Every numeric *_ms key any statement carries. Used both for `auto` and to
# validate an explicit field, so a typo aborts instead of masquerading as a
# negative result.
def is_metric(v):
    # bool is a subclass of int, so `"min_ms": true` would otherwise sum as 1;
    # a non-finite value would poison the total into nan and reach exit 1.
    return (isinstance(v, (int, float)) and not isinstance(v, bool)
            and math.isfinite(v))


# Every *_ms key that is PRESENT must be a valid metric. Checking only values
# that are already int/float would miss a string or null timing entirely: those
# are not int/float, so they would be dropped from `present` in silence and the
# remaining statements could still print EXACT -- a total over a quietly smaller
# set, which is the mistake this script exists to prevent. A statement that
# failed omits its *_ms keys altogether (verified against the reference report),
# so this rejects corruption without rejecting a legitimate failure.
for _sid, _r in rows.items():
    for _k, _v in _r.items():
        if _k.endswith("_ms") and not is_metric(_v):
            abort(f"statement {_sid} has a non-numeric or non-finite {_k}: {_v!r}.",
                  "A statement that did not run omits its timing keys entirely; a",
                  "present-but-unusable value is corruption, and dropping it would",
                  "compute a total over a quietly smaller set of statements.")

available = sorted({
    k for r in rows.values() for k, v in r.items()
    if k.endswith("_ms") and is_metric(v)
})

if field == "auto":
    if not available:
        abort("no numeric *_ms keys on any statement, so `auto` has nothing to sum.")
    fields = available
else:
    if field not in available:
        abort(f"field {field!r} is not a numeric metric on any statement.",
              f"available: {', '.join(available) if available else '(none)'}",
              "A misspelled field would otherwise report 'no basis reproduces",
              "it', which reads as evidence the figure is wrong.")
    fields = [field]

print(f"report        : {report}")
print(f"statements    : {len(rows)}")
print(f"claimed total : {claimed:.2f} s")
if expect_n is not None:
    delta = len(rows) - expect_n
    if delta > 0:
        shape = f"the basis drops {delta}"
    elif delta == 0:
        shape = "same count, so the basis is the full set if it reproduces at all"
    else:
        shape = (f"the headline claims {-delta} MORE than the report holds, so it "
                 "cannot be a subset of this report")
    print(f"claimed count : {expect_n}  (report has {len(rows)}, {shape})")
print()

TOL = 0.02  # seconds; an exact match on a figure published to 2 decimals
found_any = False


def count_note(surviving):
    if expect_n is None or surviving == expect_n:
        return ""
    return f"  [WARNING: yields {surviving} statements, headline claims {expect_n}]"


for f in fields:
    present = {k: v for k, v in rows.items() if is_metric(v.get(f))}
    missing = sorted(set(rows) - set(present))
    if not present:
        # Unreachable for an explicit field (validated above) and for `auto`
        # (drawn from the same set), but keep it explicit rather than silent.
        abort(f"field {f!r} is present on no statement.")
    total = sum(present[k][f] for k in present) / 1000.0

    print(f"--- field {f}  ({len(present)} statements carry it"
          + (f"; missing on {missing}" if missing else "") + ")")
    print(f"    full set ({len(present)}): {total:8.2f} s   residual {total - claimed:+8.2f}")

    if abs(total - claimed) <= TOL:
        print(f"    EXACT: the claimed total is the FULL set under {f}."
              f"{count_note(len(present))}")
        if not count_note(len(present)):
            print("           No subset rule needed.")
        found_any = True

    cands = []
    for k in present:
        t = (sum(present[j][f] for j in present if j != k)) / 1000.0
        cands.append((abs(t - claimed), k, t))
    cands.sort()

    hits = [c for c in cands if c[0] <= TOL]
    for _d, k, t in hits:
        print(f"    EXACT: drop {k:34} -> {t:8.2f} s  "
              f"({len(present)} -> {len(present)-1}){count_note(len(present)-1)}")
        found_any = True

    if not hits:
        print("    no single-statement drop reproduces it. Nearest three:")
        for _d, k, t in cands[:3]:
            print(f"      drop {k:34} -> {t:8.2f} s   residual {t - claimed:+8.2f}")
    print()

print("=" * 72)
if found_any:
    print("A basis that reproduces the figure was FOUND. The number is")
    print("arithmetically sound on that basis. Whether that basis is the right one")
    print("to publish is a separate judgement -- and the answer is usually written")
    print("down: grep the publishing document (epic body, ADR, report header) for")
    print("its own stated basis BEFORE filing anything.")
    sys.exit(0)

print("No single-statement basis reproduces the figure.")
print("That is NOT yet evidence the figure is wrong. Before filing, still:")
print("  1. grep the publishing document for its own stated basis -- it may name")
print("     a rule this script does not search (a matched set across systems, a")
print("     different metric, a different pass);")
print("  2. try `auto` for the field if you have not;")
print("  3. consider multi-statement drops, which this deliberately does not")
print("     search because the combinations are large and a coincidental match")
print("     would be worse than no answer.")
print()
print("If you file after this, QUOTE THIS OUTPUT in the filing.")
sys.exit(1)
PY
