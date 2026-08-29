#!/usr/bin/env bash
# Reproduce a published total from the bench report it came from.
#
# Exists because a published headline was disputed as "unreconstructable" when
# it was reconstructable: the run measured 42 statements and the headline quoted
# 41, and dropping one statement reproduced the published cold total to the cent.
# The reconstruction takes seconds; the wrong dispute cost a filing, a
# correction, and a planning target that moved the wrong way for two turns.
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
# in the publishing document. Hence the reminder this prints at the end: grep the
# publishing document for its own stated basis before concluding anything.
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
import json, os, sys

report = os.environ["REPORT"]
field = os.environ["FIELD"]
claimed = float(os.environ["CLAIMED"])
expect_n = os.environ.get("EXPECT_N") or ""
expect_n = int(expect_n) if expect_n.strip() else None

# Bench output is JSON-LINES. json.load() on the whole file raises "Extra data"
# on the second document, which has cost real time in this repo more than once.
docs, buf = [], ""
for line in open(report, errors="replace"):
    buf += line
    try:
        docs.append(json.loads(buf))
        buf = ""
    except Exception:
        pass
if not docs:
    print("ABORT: no parseable JSON documents in the report.")
    sys.exit(3)


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
    for n in walk(d):
        sid = n.get("id")
        if isinstance(sid, str) and sid[:1] == "q":
            rows[sid] = n

if not rows:
    print("ABORT: no statement nodes (objects with a string `id` starting 'q').")
    sys.exit(3)

# Which metrics can we sum? A statement missing the field is excluded from that
# metric's search rather than silently counted as zero -- a zero here would
# reproduce a lower total for the wrong reason, which is the exact class of
# mistake this script exists to prevent.
if field == "auto":
    fields = sorted({
        k for r in rows.values() for k, v in r.items()
        if k.endswith("_ms") and isinstance(v, (int, float))
    })
    if not fields:
        print("ABORT: no numeric *_ms keys found on any statement.")
        sys.exit(3)
else:
    fields = [field]

print(f"report        : {report}")
print(f"statements    : {len(rows)}")
print(f"claimed total : {claimed:.2f} s")
if expect_n is not None:
    print(f"claimed count : {expect_n}  (report has {len(rows)}, so the basis drops {len(rows) - expect_n})")
print()

TOL = 0.02  # seconds; an exact match on a figure published to 2 decimals
found_any = False

for f in fields:
    present = {k: v for k, v in rows.items() if isinstance(v.get(f), (int, float))}
    missing = sorted(set(rows) - set(present))
    if not present:
        continue
    total = sum(present[k][f] for k in present) / 1000.0

    print(f"--- field {f}  ({len(present)} statements carry it"
          + (f"; missing on {missing}" if missing else "") + ")")
    print(f"    full set ({len(present)}): {total:8.2f} s   residual {total - claimed:+8.2f}")

    if abs(total - claimed) <= TOL:
        print(f"    EXACT: the claimed total is the FULL set under {f}. No subset rule needed.")
        found_any = True

    # Single-statement drops, ranked by how close they land.
    cands = []
    for k in present:
        t = (sum(present[j][f] for j in present if j != k)) / 1000.0
        cands.append((abs(t - claimed), k, t))
    cands.sort()

    hits = [c for c in cands if c[0] <= TOL]
    for d, k, t in hits:
        note = ""
        if expect_n is not None and len(present) - 1 != expect_n:
            note = f"  [WARNING: yields {len(present)-1} statements, headline claims {expect_n}]"
        print(f"    EXACT: drop {k:34} -> {t:8.2f} s  ({len(present)} -> {len(present)-1}){note}")
        found_any = True

    if not hits:
        print("    no single-statement drop reproduces it. Nearest three:")
        for d, k, t in cands[:3]:
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
