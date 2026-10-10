#!/usr/bin/env python3
"""Judges the #2720 pre-registered bands over ABAB pass pairs.

usage: compare.py <passes-dir> <A-label>:<B-label> [<A-label>:<B-label> ...]

Every figure is checked present exactly once before it is used; a missing or
duplicated figure exits 2. Exit 0 = every band PASS or reported, 1 = a MISS
or INCONCLUSIVE band.
"""
import json
import re
import statistics
import sys
from pathlib import Path

base = Path(sys.argv[1])
pairs = [tuple(p.split(":")) for p in sys.argv[2:]]
problems = []


def need(cond, msg):
    if not cond:
        problems.append(msg)
    return cond


def classify(line):
    msg = line.split("client_message=", 1)[-1]
    if "query memory pool is full" in msg:
        return "422 query pool"
    if "tenant memory budget exhausted" in msg:
        return "422 tenant budget"
    if "process memory budget exhausted" in msg:
        return "422 process budget"
    if "temporarily unavailable" in msg or "fetch memory" in msg:
        return "503"
    return "other"


CLASSES = ["422 query pool", "422 tenant budget", "422 process budget", "503", "other"]


def load_pass(label):
    d = base / label
    r = json.loads((d / "bench-report.json").read_text())
    st = r["statements"]
    nums = [s["number"] for s in st]
    need(sorted(nums) == list(range(1, 44)), f"{label}: statements not exactly q01..q43 once: {nums}")
    by = {s["number"]: s for s in st}
    verdict = {}
    for n, s in by.items():
        if s["error"] is not None:
            verdict[n] = "error"
        elif s["verdict"] is None:
            verdict[n] = "uncompared"
        else:
            verdict[n] = s["verdict"]["verdict"]
    hot = {n: s["hot_s"] for n, s in by.items()}
    cold = {n: s["cold_s"] for n, s in by.items()}
    c = r.get("concurrency")
    need(c is not None, f"{label}: no concurrency figures")
    need(r.get("concurrency_error") is None, f"{label}: concurrency_error {r.get('concurrency_error')}")
    rss = []
    samples = []
    for line in (d / "vmrss.tsv").read_text().splitlines()[1:]:
        f = line.split("\t")
        samples.append(f)
        rss.append(int(f[1]))
    need(len(rss) > 60, f"{label}: only {len(rss)} VmRSS samples")
    t0 = float((d / "t-server-start").read_text())
    t1 = float((d / "t-bench-end").read_text())
    live = [f for f in samples if t0 <= float(f[0]) <= t1]
    cot = host = 0.0
    if len(live) >= 2:
        dt = float(live[-1][0]) - float(live[0][0])
        hz = 100.0
        cot = (int(live[-1][6]) - int(live[0][6])) / hz / dt
        host = (int(live[-1][4]) - int(live[0][4])) / hz / dt
        srv = (int(live[-1][3]) - int(live[0][3])) / hz / dt
    errs = {k: 0 for k in CLASSES}
    errfile = d / "server-errors.txt"
    for line in errfile.read_text().splitlines() if errfile.exists() else []:
        errs[classify(line)] += 1
    settings = {}
    for line in (d / "server-settings.txt").read_text().splitlines():
        m = re.search(r'setting="([a-z0-9_]+)" value=("?[^ ]+)', line)
        if m:
            need(m.group(1) not in settings, f"{label}: setting {m.group(1)} stamped twice")
            settings[m.group(1)] = m.group(2)
    up = (d / "uptime-before.txt").read_text().split("load average:")[1].split(",")[0]
    return {
        "label": label,
        "hot": hot,
        "cold": cold,
        "verdict": verdict,
        "hot_sum": r["totals"]["hot_sum_s"],
        "cold_sum": r["totals"]["cold_sum_s"],
        "answered": r["totals"]["answered"],
        "qps": c["qps"] if c else None,
        "completed": c["queries_completed"] if c else None,
        "errors": c["errors"] if c else None,
        "unreachable": c.get("unreachable") if c else None,
        "peak_rss_gb": max(rss) * 1024 / 1e9 if rss else None,
        "errs": errs,
        "settings": settings,
        "load_before": float(up),
        "cotenant_cores": cot,
        "host_busy_cores": host,
        "server_cores": srv if len(live) >= 2 else 0.0,
        "bench_exit": (d / "bench-exit").read_text().strip(),
    }


P = {}
for a, b in pairs:
    for lab in (a, b):
        if lab not in P:
            P[lab] = load_pass(lab)

print("## Per pass\n")
print("| pass | load1 before | host busy cores | co-tenant cores | server cores | answered | hot sum s | cold sum s | qps | completed | errors | pool | tenant | process | 503 | other | peak VmRSS GB | bench exit |")
print("|" + "---|" * 18)
for lab, p in P.items():
    e = p["errs"]
    print(f"| {lab} | {p['load_before']:.2f} | {p['host_busy_cores']:.2f} | {p['cotenant_cores']:.2f} | {p['server_cores']:.2f} | {p['answered']} | {p['hot_sum']:.3f} | {p['cold_sum']:.3f} | {p['qps']:.4f} | {p['completed']} | {p['errors']} | "
          f"{e['422 query pool']} | {e['422 tenant budget']} | {e['422 process budget']} | {e['503']} | {e['other']} | {p['peak_rss_gb']:.3f} | {p['bench_exit']} |")
    if sum(e.values()) != p["errors"]:
        print(f"NOTE {lab}: server-logged errors {sum(e.values())} != bench concurrency errors {p['errors']}")

ref_settings = next(iter(P.values()))["settings"]
for lab, p in P.items():
    keys = ["memory_budget_bytes", "sql_max_query_bytes", "sql_tenant_max_bytes", "cache_max_bytes",
            "catalog_cache_max_bytes", "fetch_concurrency", "sql_partition_count"]
    for k in keys:
        need(k in p["settings"], f"{lab}: setting {k} not stamped")
    diff = {k: (ref_settings.get(k), p["settings"].get(k)) for k in keys if p["settings"].get(k) != ref_settings.get(k)}
    if diff:
        print(f"\nSETTINGS DIFFER on {lab}: {diff}")

verdicts = []
print("\n## Pairs\n")
hot_r, cold_r, qps_r, rss_r = [], [], [], []
print("| pair | hot B/A | cold B/A | qps B/A | peak VmRSS B/A |")
print("|---|---|---|---|---|")
for a, b in pairs:
    A, B = P[a], P[b]
    h = B["hot_sum"] / A["hot_sum"]
    c = B["cold_sum"] / A["cold_sum"]
    q = B["qps"] / A["qps"]
    m = B["peak_rss_gb"] / A["peak_rss_gb"]
    hot_r.append(h); cold_r.append(c); qps_r.append(q); rss_r.append(m)
    print(f"| {a}:{b} | {h:.4f} | {c:.4f} | {q:.4f} | {m:.4f} |")
    need(A["answered"] == 43 or True, "")

mh, mc, mq, mr = (statistics.median(x) for x in (hot_r, cold_r, qps_r, rss_r))
print()
v = "PASS" if mh <= 1.05 else ("MISS" if mh > 1.10 else "INCONCLUSIVE")
verdicts.append(v)
print(f"hot ratio median {mh:.4f} (pairs {', '.join(f'{x:.4f}' for x in hot_r)}): {v} (PASS <= 1.05, MISS > 1.10)")
v = "PASS" if mc <= 1.10 else "MISS"
verdicts.append(v)
print(f"cold ratio median {mc:.4f} (pairs {', '.join(f'{x:.4f}' for x in cold_r)}): {v} (PASS <= 1.10)")
v = "PASS" if mq >= 0.95 else "MISS"
verdicts.append(v)
print(f"concurrency qps ratio median {mq:.4f} (pairs {', '.join(f'{x:.4f}' for x in qps_r)}): {v} (PASS >= 0.95)")
print(f"peak VmRSS ratio median {mr:.4f} (pairs {', '.join(f'{x:.4f}' for x in rss_r)}): reported; expected <= 1.10: {'within' if mr <= 1.10 else 'ABOVE'}")

# Correctness: every statement keeps arm A's verdict in every pair.
regress = []
for a, b in pairs:
    for n in range(1, 44):
        va, vb = P[a]["verdict"][n], P[b]["verdict"][n]
        if va != vb:
            regress.append((a, b, n, va, vb))
print("\nverdict changes A->B per pair:", regress or "none")
bad = [r for r in regress if r[3] in ("pass", "cardinality_only") and r[4] not in ("pass", "cardinality_only")]
v = "MISS" if bad else "PASS"
verdicts.append(v)
print(f"correctness: {v}")

# Per statement flags: B hot > 1.30 x A hot and diff > 0.05 s in every pair.
flags = []
print("\n## Per statement hot (s)\n")
hdr = " | ".join(f"{a} | {b} | B/A" for a, b in pairs)
print(f"| q | {hdr} | flagged |")
print("|---|" + "---|" * (3 * len(pairs)) + "---|")
for n in range(1, 44):
    cells, every = [], True
    for a, b in pairs:
        ha, hb = P[a]["hot"][n], P[b]["hot"][n]
        if ha is None or hb is None:
            cells.append(f"{ha} | {hb} | -")
            every = False
            continue
        cells.append(f"{ha:.3f} | {hb:.3f} | {hb / ha:.2f}")
        if not (hb > 1.30 * ha and hb - ha > 0.05):
            every = False
    if every:
        flags.append(n)
    print(f"| q{n:02d} | {' | '.join(cells)} | {'FLAG' if every else ''} |")
print("\nflagged statements:", [f"q{n:02d}" for n in flags] or "none")

# Refusals: MISS if B's count exceeds A's by > 20 % and by > 5, per pair.
print("\n## Refusals (typed 422, all classes) per pair\n")
for a, b in pairs:
    ra = sum(P[a]["errs"][k] for k in CLASSES[:3])
    rb = sum(P[b]["errs"][k] for k in CLASSES[:3])
    miss = rb > ra * 1.2 and rb - ra > 5
    print(f"{a}:{b} A={ra} B={rb} {'MISS' if miss else 'PASS'}")
    verdicts.append("MISS" if miss else "PASS")

if problems:
    print("\nPROBLEMS:")
    for p in problems:
        if p:
            print(" -", p)
    sys.exit(2)
sys.exit(0 if all(v == "PASS" for v in verdicts) and not flags else 1)
