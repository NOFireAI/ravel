#!/usr/bin/env python3
"""Issue #2633 sampler: every 5 s scrape /metrics and /proc/<pid>/status.

One TSV row per sample. Fixed columns are derived from the families by
parsing labels (not by label order); the last column keeps every raw series
of the scraped families verbatim, so nothing is lost.

usage: sampler.py <metrics-url> <pid> <out.tsv> [interval-s]
"""
import re
import sys
import time
import urllib.request

FAMILIES = (
    "ravel_process_allocator_bytes",
    "ravel_memory_reserved_bytes",
    "ravel_cache_resident_bytes",
    "ravel_memory_handoff_overlap_bytes",
    "ravel_memory_budget_bytes",
)
LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(\S+)")
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

COLUMNS = [
    "unix_ts", "ok", "allocated", "active", "resident",
    "sql_reserved", "fetch_reserved", "fetch_cache_resident",
    "catalog_cache_resident", "handoff_overlap", "budget_limit",
    "accounted", "gap", "vmrss_bytes", "rssanon_bytes", "raw_series",
]


def proc_status(pid):
    out = {}
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                key, _, rest = line.partition(":")
                if key in ("VmRSS", "RssAnon"):
                    out[key] = int(rest.split()[0]) * 1024
    except OSError:
        pass
    return out


def scrape(url):
    with urllib.request.urlopen(url, timeout=4) as resp:
        text = ANSI.sub("", resp.read().decode("utf-8", "replace"))
    vals = {}
    raw = []
    for line in text.splitlines():
        m = LINE.match(line)
        if not m or m.group(1) not in FAMILIES:
            continue
        name, labels, value = m.group(1), dict(LABEL.findall(m.group(3) or "")), float(m.group(4))
        raw.append(f"{name}{{{m.group(3) or ''}}}={m.group(4)}")
        if name == "ravel_process_allocator_bytes":
            vals[labels.get("stat", "?")] = value
        elif name == "ravel_memory_reserved_bytes":
            vals[labels.get("component", "?") + "_reserved"] = value
        elif name == "ravel_cache_resident_bytes":
            # Sum every tier of a cache family (RAM-only here; a disk tier
            # would carry a tier label).
            key = labels.get("cache", "?") + "_cache_resident"
            vals[key] = vals.get(key, 0.0) + value
        elif name == "ravel_memory_handoff_overlap_bytes":
            vals["handoff_overlap"] = value
        elif name == "ravel_memory_budget_bytes":
            vals["budget_limit"] = value
    return vals, raw


def main():
    url, pid, out = sys.argv[1], sys.argv[2], sys.argv[3]
    interval = float(sys.argv[4]) if len(sys.argv) > 4 else 5.0
    with open(out, "a", buffering=1) as f:
        if f.tell() == 0:
            f.write("\t".join(COLUMNS) + "\n")
        next_t = time.time()
        while True:
            ts = time.time()
            st = proc_status(pid)
            try:
                v, raw = scrape(url)
                ok = 1
            except Exception as e:  # keep sampling through a slow scrape
                v, raw, ok = {}, [f"scrape_error={type(e).__name__}"], 0
            g = lambda k: v.get(k)
            accounted = None
            if ok and all(g(k) is not None for k in
                          ("sql_reserved", "fetch_reserved", "fetch_cache_resident", "catalog_cache_resident")):
                accounted = g("sql_reserved") + g("fetch_reserved") + g("fetch_cache_resident") + g("catalog_cache_resident")
            gap = g("resident") - accounted if accounted is not None and g("resident") is not None else None
            row = [f"{ts:.3f}", str(ok)]
            for k in ("allocated", "active", "resident", "sql_reserved", "fetch_reserved",
                      "fetch_cache_resident", "catalog_cache_resident", "handoff_overlap", "budget_limit"):
                row.append("" if g(k) is None else str(int(g(k))))
            row.append("" if accounted is None else str(int(accounted)))
            row.append("" if gap is None else str(int(gap)))
            row.append(str(st.get("VmRSS", "")))
            row.append(str(st.get("RssAnon", "")))
            row.append(";".join(raw))
            f.write("\t".join(row) + "\n")
            if not st:
                f.write(f"# {ts:.3f} pid {pid} gone\n")
                return
            next_t += interval
            time.sleep(max(0.0, next_t - time.time()))


if __name__ == "__main__":
    main()
