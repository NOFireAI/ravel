#!/usr/bin/env python3
"""Pre-registered rows for the #2633 aggregate-hold run.

Run in a run directory (samples.tsv, bench-report.json, t-bench-end, and
optionally server-errors.txt). Row 1 is the per-sample maximum of
`allocated - accounted + handoff_overlap` over every live sample, with
accounted the sampler's pre-registered sum (SQL and fetch reservations plus
fetch and catalog cache resident). Adding handoff_overlap back undoes the
double count of the fetch bytes also charged to the cache.

Exits non-zero when a figure is missing, so a short series cannot print a
partial table that reads as complete.
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
NEED = ("allocated", "resident", "accounted", "handoff_overlap", "vmrss_bytes")


def load_rows():
    rows = []
    for line in open("samples.tsv"):
        if line.startswith("#") or line.startswith("unix_ts"):
            continue
        f = line.rstrip("\n").split("\t")
        rows.append({c: (float(f[i]) if f[i] != "" else None) for i, c in enumerate(COLS)})
    return rows


def unc(d):
    return d["allocated"] - d["accounted"] + d["handoff_overlap"]


def main():
    rows = load_rows()
    live = [d for d in rows if d["ok"] == 1 and all(d[k] is not None for k in NEED)]
    skipped = [d["unix_ts"] for d in rows if d not in live]
    bend = float(open("t-bench-end").read())
    rep = json.load(open("bench-report.json"))
    conc = rep["concurrency"]
    cs = bend - conc["elapsed_s"]
    phase = [d for d in live if cs <= d["unix_ts"] <= bend]
    if not live or not phase:
        print("no live or no concurrency-phase samples")
        return 1
    print(f"samples {len(rows)}, live {len(live)}, not live {len(skipped)} {skipped}; "
          f"phase samples {len(phase)}; concurrency start {cs:.3f}, bench end {bend:.3f}")

    pk = max(live, key=unc)
    where = "phase" if cs <= pk["unix_ts"] <= bend else ("serial" if pk["unix_ts"] < cs else "idle")
    print(f"ROW1 max(allocated - accounted + handoff_overlap) {unc(pk) / G:.3f} GB "
          f"at unix {pk['unix_ts']:.3f} (t=+{pk['unix_ts'] - cs:.1f}s, {where})")
    print("  components: " + ", ".join(
        f"{k} {pk[k] / G:.3f}" for k in ("allocated", "accounted", "handoff_overlap",
                                         "sql_reserved", "fetch_reserved",
                                         "fetch_cache_resident", "catalog_cache_resident",
                                         "resident", "vmrss_bytes")) + " GB")
    top = sorted(live, key=unc, reverse=True)[:5]
    print("  top 5: " + "; ".join(f"t=+{d['unix_ts'] - cs:.1f}s {unc(d) / G:.3f}" for d in top))
    us = sorted(unc(d) for d in phase)
    print(f"ROW2 median over phase {st.median(us) / G:.3f} GB ({len(us)} samples), "
          f"p90 {us[int(0.9 * (len(us) - 1))] / G:.3f} GB, "
          f"samples >= 2.0 GB: {sum(1 for u in us if u >= 2.0 * G)}, "
          f">= 3.0 GB: {sum(1 for u in us if u >= 3.0 * G)}")

    print(f"ROW3 concurrency qps {conc['qps']:.3f}, completed {conc['queries_completed']}, "
          f"errors {conc['errors']}, unreachable {conc['unreachable']}")

    if os.path.exists("server-errors.txt"):
        typed = other = 0
        for line in open("server-errors.txt"):
            m = re.search(r" error=(.*)$", line)
            if not m:
                continue
            msg = m.group(1)
            if "fetch memory exhausted" in msg:
                other += 1
            elif "query memory budget exhausted" in msg:
                typed += 1
            else:
                other += 1
        print(f"ROW4 typed 422 refusals {typed}; other server-logged query errors {other}; "
              f"bench-counted errors {conc['errors']}")
    else:
        print("ROW4 server-errors.txt missing")
        return 1

    pr = max(live, key=lambda d: d["resident"] - d["allocated"])
    prp = max(phase, key=lambda d: d["resident"] - d["allocated"])
    pv = max(live, key=lambda d: d["vmrss_bytes"])
    print(f"ROW5 peak retention (resident - allocated) {(pr['resident'] - pr['allocated']) / G:.3f} GB "
          f"at t=+{pr['unix_ts'] - cs:.1f}s (phase peak {(prp['resident'] - prp['allocated']) / G:.3f} GB "
          f"at t=+{prp['unix_ts'] - cs:.1f}s); peak VmRSS {pv['vmrss_bytes'] / G:.3f} GB "
          f"at t=+{pv['unix_ts'] - cs:.1f}s; mean VmRSS over phase "
          f"{st.mean(d['vmrss_bytes'] for d in phase) / G:.3f} GB")
    serial_err = [(s["number"], s["error"]) for s in rep["statements"] if s["error"]]
    print(f"VALIDITY serial statements {len(rep['statements'])}, errored {len(serial_err)}: {serial_err}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
