#!/usr/bin/env python3
"""Issue #2633 charging run: one statement at a time against ravel-server.

For each run, a 20 ms /metrics sampler records jemalloc `allocated`, the SQL
and fetch reservations and cache residency. The statement is POSTed to
/api/v1/sql; the allocation peak is the largest `allocated` inside the
statement's window minus the median of the 10 samples before it. The
per-consumer peak table is the `pool consumer peaks` line the instrumented
TenantDelegatingPool logs when it drops (task-branch patch).

usage: charge.py <base-url> <token-file> <server-log> <out.jsonl> <reps> <q> [<q> ...]
  The first run of each statement is a warm-up (rep 0) and is recorded but
  excluded from the summaries. Exits non-zero if any run fails, any run
  has fewer than 5 samples in its window, or a run has no pool dump.
"""
import json
import re
import statistics
import sys
import threading
import time
import urllib.request

ROOT = "/var/lib/fleet/work/428ccefa-2a93-4412-ae02-50fc82265f78"
QUERIES = open(f"{ROOT}/benchmarks/clickbench/parquet/queries.sql").read().splitlines()
ANSI = re.compile(r"\x1b\[[0-9;]*m")
SERIES = {
    "allocated": 'ravel_process_allocator_bytes{mode="all",allocator="jemalloc",stat="allocated"}',
    "resident": 'ravel_process_allocator_bytes{mode="all",allocator="jemalloc",stat="resident"}',
    "sql": 'ravel_memory_reserved_bytes{mode="all",component="sql"}',
    "fetch": 'ravel_memory_reserved_bytes{mode="all",component="fetch"}',
}


class Sampler(threading.Thread):
    def __init__(self, url, period=0.02):
        super().__init__(daemon=True)
        self.url, self.period = url, period
        self.samples = []  # (t, dict)
        self.errors = 0
        self.stop = False

    def run(self):
        while not self.stop:
            t = time.time()
            try:
                body = urllib.request.urlopen(self.url, timeout=2).read().decode()
                vals = {}
                cache = {}
                for line in body.splitlines():
                    if line.startswith("#"):
                        continue
                    key, _, v = line.rpartition(" ")
                    for name, s in SERIES.items():
                        if key == s:
                            vals[name] = float(v)
                    if key.startswith("ravel_cache_resident_bytes{"):
                        cache[key] = float(v)
                vals["cache"] = sum(cache.values())
                self.samples.append((t, vals))
            except Exception:
                self.errors += 1
            dt = self.period - (time.time() - t)
            if dt > 0:
                time.sleep(dt)


def post(base, token, sql):
    req = urllib.request.Request(
        f"{base}/api/v1/sql",
        data=json.dumps({"query": sql}).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=500) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def pool_dumps(log, offset):
    with open(log, "rb") as f:
        f.seek(offset)
        data = f.read()
        end = f.tell()
    dumps = []
    # Not splitlines(): it also splits on the \x1e row separator in the dump.
    for raw in data.decode(errors="replace").split("\n"):
        line = ANSI.sub("", raw)
        if "pool consumer peaks" not in line:
            continue
        m = re.search(r"query_peak=(\d+) table=(.*)$", line)
        if not m:
            continue
        table = {}
        for row in m.group(2).split("\x1e"):
            parts = row.split("\t")
            if len(parts) == 3:
                table[parts[0]] = (int(parts[1]), int(parts[2]))
        dumps.append({"query_peak": int(m.group(1)), "table": table})
    return dumps, end


def main():
    base, token_file, log, out, reps = sys.argv[1:6]
    qs = [int(q) for q in sys.argv[6:]]
    token = open(token_file).read().strip()
    sampler = Sampler(f"{base}/metrics")
    sampler.start()
    time.sleep(1.0)
    offset = open(log, "rb").seek(0, 2)
    bad = 0
    with open(out, "a") as fo:
        for q in qs:
            sql = QUERIES[q - 1]
            for rep in range(int(reps) + 1):
                time.sleep(1.0)
                pre_samples = [s for s in sampler.samples[-10:]]
                t0 = time.time()
                code, body = post(base, token, sql)
                t1 = time.time()
                time.sleep(0.3)
                win_t = [(t, v) for t, v in sampler.samples if t0 <= t <= t1 + 0.1]
                win = [v for _, v in win_t]
                pre_alloc = statistics.median(v["allocated"] for _, v in pre_samples)
                pre_cache = statistics.median(v["cache"] for _, v in pre_samples)
                time.sleep(0.5)
                dumps, offset = pool_dumps(log, offset)
                rec = {
                    "q": q,
                    "rep": rep,
                    "t0": t0,
                    "t1": t1,
                    "secs": round(t1 - t0, 3),
                    "http": code,
                    "n_samples": len(win),
                    "pre_allocated": pre_alloc,
                    "peak_allocated": max((v["allocated"] for v in win), default=None),
                    "peak_sql": max((v["sql"] for v in win), default=None),
                    "peak_fetch": max((v["fetch"] for v in win), default=None),
                    "peak_resident": max((v["resident"] for v in win), default=None),
                    "cache_delta": (max((v["cache"] for v in win), default=pre_cache) - pre_cache),
                    "max_simul_uncharged": max(
                        (v["allocated"] - pre_alloc - v["sql"] - v["fetch"] for v in win), default=None
                    ),
                    "dumps": dumps,
                    "series": [
                        (round(t, 3), v["allocated"], v["sql"], v["fetch"]) for t, v in win_t
                    ],
                }
                if win:
                    rec["alloc_delta"] = rec["peak_allocated"] - pre_alloc
                    rec["gap"] = rec["alloc_delta"] - rec["peak_sql"]
                if code != 200:
                    rec["error"] = body[:400].decode(errors="replace")
                    bad += 1
                if len(win) < 5 or not dumps:
                    bad += 1
                fo.write(json.dumps(rec) + "\n")
                fo.flush()
                qp = max((d["query_peak"] for d in dumps), default=-1)
                print(
                    f"q{q:02d} rep{rep} http={code} {rec['secs']}s n={len(win)} "
                    f"delta={rec.get('alloc_delta', 0)/1e9:.3f}GB sql={(rec['peak_sql'] or 0)/1e9:.3f}GB "
                    f"gap={rec.get('gap', 0)/1e9:.3f}GB pool_peak={qp/1e9:.3f}GB dumps={len(dumps)} "
                    f"cache_d={rec['cache_delta']/1e9:.3f}GB",
                    flush=True,
                )
    sampler.stop = True
    print(f"sampler errors={sampler.errors} bad_runs={bad}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
