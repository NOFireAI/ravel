#!/usr/bin/env python3
"""Re-attach full pool dumps to runs recorded by the first charge.py version.

That version parsed the server log with str.splitlines(), which also splits on
the \\x1e row separator, so each stored dump kept only its first (largest)
row. query_peak was unaffected. This re-reads every dump line from the server
log in order, takes the last len(runs) of them, checks each one's query_peak
against the run it replaces, and rewrites the dumps.

usage: redump.py <server.log> <runs.jsonl> <out.jsonl>
Exits non-zero on any query_peak mismatch.
"""
import json
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from charge import pool_dumps  # noqa: E402

log, runs_path, out = sys.argv[1:4]
dumps, _ = pool_dumps(log, 0)
runs = [json.loads(line) for line in open(runs_path)]
tail = dumps[-len(runs):]
bad = 0
for r, d in zip(runs, tail):
    if len(r["dumps"]) != 1 or r["dumps"][0]["query_peak"] != d["query_peak"]:
        bad += 1
    r["dumps"] = [d]
with open(out, "w") as f:
    for r in runs:
        f.write(json.dumps(r) + "\n")
print(f"log dumps={len(dumps)} runs={len(runs)} mismatches={bad}")
sys.exit(1 if bad else 0)
