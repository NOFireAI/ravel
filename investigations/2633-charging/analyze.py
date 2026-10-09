#!/usr/bin/env python3
"""Summarise runs.jsonl from charge.py.

usage: analyze.py <runs.jsonl> [<q> ...]
Prints the per-statement table (median and range over the measured runs,
rep >= 1) and, for each statement named on the command line, the per-consumer
peak table (base names, partition indices folded to [*]) of each measured run.
Exits non-zero unless every statement has exactly 3 measured runs, each
HTTP 200 with a pool dump.
"""
import json
import statistics
import sys
from collections import defaultdict

GB = 1e9


def fmt(xs):
    return f"{statistics.median(xs)/GB:.3f} ({min(xs)/GB:.3f}-{max(xs)/GB:.3f})"


def main():
    runs = [json.loads(line) for line in open(sys.argv[1])]
    detail = [int(q) for q in sys.argv[2:]]
    by_q = defaultdict(list)
    for r in runs:
        if r["rep"] >= 1:
            by_q[r["q"]].append(r)
    bad = 0
    print("| q | runs | alloc peak delta | SQL reservation peak | gap (alloc - SQL) | fetch reservation peak | max simultaneous alloc - SQL - fetch | pool peak (dump) | secs |")
    print("|---|---|---|---|---|---|---|---|---|")
    for q in sorted(by_q):
        rs = by_q[q]
        if len(rs) != 3 or any(r["http"] != 200 or not r["dumps"] for r in rs):
            bad += 1
        pool = [max(d["query_peak"] for d in r["dumps"]) for r in rs]
        print(
            f"| q{q:02d} | {len(rs)} | {fmt([r['alloc_delta'] for r in rs])} | {fmt([r['peak_sql'] for r in rs])} "
            f"| **{fmt([r['gap'] for r in rs])}** | {fmt([r['peak_fetch'] for r in rs])} "
            f"| {fmt([r['max_simul_uncharged'] for r in rs])} | {fmt(pool)} "
            f"| {statistics.median(r['secs'] for r in rs):.3f} |"
        )
    for q in detail:
        rs = by_q[q]
        names = defaultdict(list)
        for r in rs:
            d = max(r["dumps"], key=lambda d: d["query_peak"])
            for k, (peak, cur) in d["table"].items():
                if k.startswith("B "):
                    names[k[2:]].append(peak)
        print(f"\n### q{q:02d} per-consumer peaks (GB, partition indices folded; runs 1-3)\n")
        print("| consumer | run 1 | run 2 | run 3 | median |")
        print("|---|---|---|---|---|")
        for name, peaks in sorted(names.items(), key=lambda kv: -statistics.median(kv[1])):
            if max(peaks) == 0:
                continue
            cells = " | ".join(f"{p/GB:.3f}" for p in peaks)
            print(f"| `{name}` | {cells} | {statistics.median(peaks)/GB:.3f} |")
        zero = sorted(n for n, p in names.items() if max(p) == 0)
        print(f"\nConsumers registered with a zero peak: {', '.join('`'+z+'`' for z in zero) or 'none'}")
        parts = defaultdict(list)
        for r in rs:
            d = max(r["dumps"], key=lambda d: d["query_peak"])
            n = sum(1 for k in d["table"] if k.startswith("P "))
            parts["P"].append(n)
        print(f"Per-partition consumer names per run: {parts['P']}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
