#!/usr/bin/env python3
"""Per-arm figures for the #2633 decay experiment that analyze.py does not print.

Run in an arm directory next to analyze.py's inputs (samples.tsv,
bench-report.json, t-server-start, t-bench-end). Optionally reads
server-errors.txt, the server's "sql query error redacted" WARN lines with
ANSI codes stripped, to classify every concurrency error rather than only
each statement's first one.

Exits non-zero when a figure the README reports is missing, so a short
sample series cannot print a partial table that reads as complete.
"""
import json
import os
import re
import statistics as st
import sys

G = 1e9
COLS = ("unix_ts ok allocated active resident sql_reserved fetch_reserved "
        "fetch_cache_resident catalog_cache_resident handoff_overlap budget_limit "
        "accounted gap vmrss_bytes rssanon_bytes bg_thread").split()


def load_rows():
    rows = []
    for line in open("samples.tsv"):
        if line.startswith("#") or line.startswith("unix_ts"):
            continue
        f = line.rstrip("\n").split("\t")
        rows.append({c: (float(f[i]) if f[i] != "" else None) for i, c in enumerate(COLS)})
    return rows


def kind(msg):
    if "fetch memory exhausted" in msg:
        return "fetch memory exhausted (client sees HTTP 503 unavailable)"
    if "spill is disabled" in msg:
        return "422 query pool full, spill disabled"
    if "tenant memory budget exhausted" in msg:
        return "422 tenant memory budget exhausted"
    if "process memory budget exhausted" in msg:
        return "422 process memory budget exhausted"
    return "other: " + msg[:120]


def main():
    fail = []
    rows = [d for d in load_rows() if d["ok"] == 1]
    bend = float(open("t-bench-end").read())
    rep = json.load(open("bench-report.json"))
    conc = rep["concurrency"]
    cs = bend - conc["elapsed_s"]
    phase = [d for d in rows if cs <= d["unix_ts"] <= bend]
    if not phase:
        print("no concurrency-phase samples")
        return 1

    serial_err = [(s["number"], s["error"]) for s in rep["statements"] if s["error"]]
    print(f"serial statements {len(rep['statements'])}, errored {len(serial_err)}: {serial_err}")
    bg_bad = sum(1 for d in rows if d["bg_thread"] != 1)
    print(f"sampled rows {len(rows)}, rows with bg_thread != 1: {bg_bad}")

    pk = max(phase, key=lambda d: d["gap"])
    ret = pk["resident"] - pk["allocated"]
    unc = pk["allocated"] - pk["accounted"]
    print(f"peak gap {pk['gap'] / G:.3f} GB at t=+{pk['unix_ts'] - cs:.1f}s: "
          f"retention {ret / G:.3f} GB ({ret / pk['gap']:.1%}), "
          f"uncharged live {unc / G:.3f} GB ({unc / pk['gap']:.1%}), "
          f"resident-active {(pk['resident'] - pk['active']) / G:.3f} GB, "
          f"handoff overlap {pk['handoff_overlap'] / G:.3f} GB")
    rets = sorted(d["resident"] - d["allocated"] for d in phase)
    print(f"median retention over phase {st.median(rets) / G:.3f} GB "
          f"({len(phase)} samples), p90 {rets[int(0.9 * (len(rets) - 1))] / G:.3f} GB, "
          f"samples >= 1.5 GB: {sum(1 for r in rets if r >= 1.5 * G)}")
    print(f"mean over phase: resident {st.mean(d['resident'] for d in phase) / G:.3f} GB, "
          f"VmRSS {st.mean(d['vmrss_bytes'] for d in phase) / G:.3f} GB")
    pr = max(phase, key=lambda d: d["resident"])
    print(f"peak resident {pr['resident'] / G:.3f} GB at t=+{pr['unix_ts'] - cs:.1f}s")
    pv = max(rows, key=lambda d: d["vmrss_bytes"] or 0)
    print(f"peak VmRSS {pv['vmrss_bytes'] / G:.3f} GB at t=+{pv['unix_ts'] - cs:.1f}s "
          f"({pv['unix_ts'] - bend:+.1f}s from bench end)")
    print(f"concurrency qps {conc['qps']:.3f}, completed {conc['queries_completed']}, "
          f"errors {conc['errors']}, unreachable {conc['unreachable']}")

    if os.path.exists("server-errors.txt"):
        kinds = {}
        for line in open("server-errors.txt"):
            m = re.search(r" error=(.*)$", line)
            if m:
                k = kind(m.group(1))
                kinds[k] = kinds.get(k, 0) + 1
        total = sum(kinds.values())
        print(f"server-logged query errors {total} (bench counted {conc['errors']}):")
        for k, n in sorted(kinds.items(), key=lambda kv: -kv[1]):
            print(f"  {n:4d} {k}")
    by_stmt = {s["number"]: s["errors"] for s in conc["statements"] if s["errors"]}
    print(f"errors by statement: {by_stmt}")

    for off in (60, 120):
        after = [d for d in rows if d["unix_ts"] - bend >= off]
        if not after:
            print(f"idle +{off}s: NO SAMPLE")
            fail.append(off)
            continue
        d = after[0]
        print(f"idle +{off}s (sample at +{d['unix_ts'] - bend:.1f}s): retention "
              f"{(d['resident'] - d['allocated']) / G:.3f} GB, resident {d['resident'] / G:.3f} GB, "
              f"allocated {d['allocated'] / G:.3f} GB, VmRSS {d['vmrss_bytes'] / G:.3f} GB")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
