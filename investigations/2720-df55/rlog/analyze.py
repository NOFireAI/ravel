#!/usr/bin/env python3
"""Pre-registered #2720 figures for the rlog lane.

Usage: analyze.py <pairs> where <pairs> is a comma-separated list of
A-pass:B-pass directory names, e.g. p1-A:p1-B,p2-A:p2-B.

Each pass directory holds progress.jsonl (one event per statement), samples.tsv
(1 s VmRSS samples) and server.log. Every figure is checked present exactly
once per statement and per pass; a violation exits 2.
"""
import json
import os
import re
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.join(HERE, "..", "..", "..", "benchmarks", "clickbench", "hits.corpus.json")
EXPECTED_IDS = [e["id"] for e in json.load(open(CORPUS))["entries"]]
assert len(EXPECTED_IDS) == 43, len(EXPECTED_IDS)

violations = []


def hot_ms(entry):
    # Hot = best of runs 2 and 3. With three runs the report carries min, median
    # and cold (run 1): if run 1 is not the minimum, the minimum is a hot run;
    # if it is, the best hot run is the median.
    if entry["min_ms"] < entry["cold_ms"]:
        return entry["min_ms"]
    return entry["median_ms"]


def load_pass(name):
    d = os.path.join(HERE, name)
    measured, failed, skipped = {}, {}, {}
    seen = {}
    for line in open(os.path.join(d, "progress.jsonl")):
        ev = json.loads(line)
        seen[ev["id"]] = seen.get(ev["id"], 0) + 1
        {"measured": measured, "failed": failed, "skipped": skipped}[ev["outcome"]][ev["id"]] = ev
    for i in EXPECTED_IDS:
        if seen.get(i, 0) != 1:
            violations.append(f"{name}: statement {i} present {seen.get(i, 0)} times, want 1")
    for i in seen:
        if i not in EXPECTED_IDS:
            violations.append(f"{name}: unexpected statement {i}")
    # Peak VmRSS from the 1 s samples.
    rss, load1 = [], []
    for line in list(open(os.path.join(d, "samples.tsv")))[1:]:
        f = line.split("\t")
        rss.append(int(f[1]))
        load1.append(float(f[3]))
    if not rss:
        violations.append(f"{name}: no VmRSS samples")
    # Refusals by class from the server log and the failed entries.
    classes = {"422_query_pool": 0, "422_tenant_budget": 0, "422_process_budget": 0, "503": 0, "other": 0}
    for ev in failed.values():
        classes[classify(ev["error"])] += 1
    return {
        "name": name,
        "measured": measured,
        "failed": failed,
        "skipped": skipped,
        "peak_rss_kb": max(rss) if rss else None,
        "samples": len(rss),
        "max_load1": max(load1) if load1 else None,
        "median_load1": statistics.median(load1) if load1 else None,
        "classes": classes,
    }


def classify(err):
    e = err.lower()
    if "query pool" in e or "query memory budget" in e:
        return "422_query_pool"
    if "tenant memory budget" in e or "tenant" in e and "budget" in e:
        return "422_tenant_budget"
    if "process memory budget" in e:
        return "422_process_budget"
    if "503" in e or "unavailable" in e or "fetch memory" in e:
        return "503"
    return "other"


def main():
    pairs = [p.split(":") for p in sys.argv[1].split(",")]
    ratios = []
    per_stmt = {}
    out = []
    for a_name, b_name in pairs:
        a, b = load_pass(a_name), load_pass(b_name)
        both = [i for i in EXPECTED_IDS if i in a["measured"] and i in b["measured"]]
        a_sum = sum(hot_ms(a["measured"][i]) for i in both)
        b_sum = sum(hot_ms(b["measured"][i]) for i in both)
        a_cold = sum(a["measured"][i]["cold_ms"] for i in both)
        b_cold = sum(b["measured"][i]["cold_ms"] for i in both)
        ratio = b_sum / a_sum
        ratios.append(ratio)
        out.append(f"== pair {a_name} vs {b_name}")
        out.append(f"statements answered by both: {len(both)}")
        out.append(f"hot sum A {a_sum/1000:.3f} s, B {b_sum/1000:.3f} s, ratio B/A {ratio:.4f}")
        out.append(f"cold sum A {a_cold/1000:.3f} s, B {b_cold/1000:.3f} s, ratio B/A {b_cold/a_cold:.4f} (reported)")
        rows_diff = [
            (i, a["measured"][i]["rows_returned"], b["measured"][i]["rows_returned"])
            for i in both
            if a["measured"][i]["rows_returned"] != b["measured"][i]["rows_returned"]
        ]
        out.append(f"row-count differences: {rows_diff if rows_diff else 'none'}")
        fa, fb = set(a["failed"]), set(b["failed"])
        out.append(f"failed A: {sorted(fa)}")
        out.append(f"failed B: {sorted(fb)}")
        out.append(f"failed only on B: {sorted(fb - fa)}; only on A: {sorted(fa - fb)}")
        out.append(f"skipped A: {sorted(a['skipped'])}; skipped B: {sorted(b['skipped'])}")
        for p in (a, b):
            out.append(
                f"{p['name']}: peak VmRSS {p['peak_rss_kb']/1048576:.3f} GiB ({p['peak_rss_kb']} kB) over {p['samples']} samples; "
                f"load1 median {p['median_load1']} max {p['max_load1']}; refusals {p['classes']}"
            )
        out.append(f"peak VmRSS B/A: {b['peak_rss_kb']/a['peak_rss_kb']:.4f}")
        out.append("per statement: id, A hot s, B hot s, B/A, A rows, B rows, A cold s, B cold s")
        for i in EXPECTED_IDS:
            if i in both:
                ah, bh = hot_ms(a["measured"][i]) / 1000, hot_ms(b["measured"][i]) / 1000
                flag = bh > 1.30 * ah and (bh - ah) > 0.05
                per_stmt.setdefault(i, []).append(flag)
                out.append(
                    f"  {i:38s} {ah:9.3f} {bh:9.3f} {bh/ah if ah else float('nan'):7.3f} "
                    f"{a['measured'][i]['rows_returned']:>9} {b['measured'][i]['rows_returned']:>9} "
                    f"{a['measured'][i]['cold_ms']/1000:9.3f} {b['measured'][i]['cold_ms']/1000:9.3f}{'  FLAG' if flag else ''}"
                )
            else:
                per_stmt.setdefault(i, []).append(None)
                out.append(f"  {i:38s} not answered by both (A {'ok' if i in a['measured'] else 'fail'}, B {'ok' if i in b['measured'] else 'fail'})")
    med = statistics.median(ratios)
    out.append("== summary")
    out.append(f"per-pair hot ratios: {[round(r, 4) for r in ratios]}; median {med:.4f}")
    verdict = "PASS" if med <= 1.05 else ("MISS" if med > 1.10 else "INCONCLUSIVE")
    out.append(f"hot verdict: {verdict}")
    flagged = [i for i, fl in per_stmt.items() if fl and all(f is True for f in fl)]
    out.append(f"statements flagged in every pair: {flagged if flagged else 'none'}")
    if violations:
        out.append("VIOLATIONS:")
        out.extend(violations)
    print("\n".join(out))
    sys.exit(2 if violations else 0)


if __name__ == "__main__":
    main()
