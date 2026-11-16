#!/usr/bin/env python3
"""Predicts the on-object leaf bytes ADR-4146 states for the cardinality tenant.

Builds the three series fields of one part from the label rules of
benchmarks/metrics/workload.json (the generator's labels_for and
series_per_instance), with random 64-bit series keys and pair keys in place
of BLAKE3 prefixes, encodes each leaf body as kidx.rs does (bucket_bits from
the decompressed body, one zstd frame per non-empty bucket at level 3 plus
its 12-byte frame header), and runs the set-probe read plan (one range per
touched bucket up to B_run, bound_runs above it) over the resulting frame
offsets. Every figure it prints is an on-object byte count unless the line
says "raw" (decompressed). Needs python3 and the zstd CLI. Deterministic for
a given seed; about a minute.

    python3 docs/adrs/assets/4146-leaf-bytes-sim.py
"""

import random
import subprocess

SEED = 4146
MIX = 0x9E3779B97F4A7C15
M64 = (1 << 64) - 1
B_RUN = 64
Q_A = 8 * 1024 * 1024
Q_BC = 4 * 1024 * 1024
E = 25_401


def bucket(key, bits):
    return ((key * MIX) & M64) >> (64 - bits)


def uv(n):
    out = bytearray()
    while True:
        low, n = n & 0x7F, n >> 7
        if n:
            out.append(low | 0x80)
        else:
            out.append(low)
            return bytes(out)


def choose_bits(body):
    best, best_diff = 1, None
    for bits in range(1, 14):
        diff = abs((4 << (2 * bits)) - body)
        if best_diff is None or diff < best_diff:
            best, best_diff = bits, diff
    return best


def zstd_len(raw):
    done = subprocess.run(
        ["zstd", "-3", "-q", "-c", "--no-check"], input=raw, capture_output=True, check=True
    )
    return len(done.stdout)


class Leaf:
    def __init__(self, name, entries):
        self.raw = sum(8 + len(p) for _, p in entries)
        self.bits = choose_bits(self.raw)
        frames = [bytearray() for _ in range(1 << self.bits)]
        for key, payload in entries:
            frames[bucket(key, self.bits)] += key.to_bytes(8, "big") + payload
        self.sizes = [12 + zstd_len(bytes(f)) if f else 0 for f in frames]
        self.starts = []
        at = 0
        for size in self.sizes:
            self.starts.append(at)
            at += size
        self.body = at
        self.prefix_dir = 4 << self.bits
        print(
            f"{name}: entries {len(entries):,} raw {self.raw:,} B, bucket_bits {self.bits}, "
            f"on-object body {self.body:,} B (ratio {self.body / self.raw:.3f}), "
            f"directory {self.prefix_dir:,} B"
        )

    def plan_bytes(self, keys):
        touched = sorted({bucket(k, self.bits) for k in keys if self.sizes[bucket(k, self.bits)]})
        runs = [(self.starts[b], self.starts[b] + self.sizes[b]) for b in touched]
        if len(runs) > B_RUN:
            runs = bound_runs(runs, B_RUN)
        return len(touched), sum(e - s for s, e in runs)


def bound_runs(runs, max_runs):
    gaps = sorted((runs[i + 1][0] - runs[i][1], i) for i in range(len(runs) - 1))
    bridged = [False] * (len(runs) - 1)
    for _, i in gaps[: len(runs) - max_runs]:
        bridged[i] = True
    out, bridge_previous = [], False
    for run, bridge_next in zip(runs, bridged + [False]):
        if out and bridge_previous:
            out[-1] = (out[-1][0], max(out[-1][1], run[1]))
        else:
            out.append(run)
        bridge_previous = bridge_next
    return out


def posting(values):
    out, previous = bytearray(uv(len(values))), -1
    for v in values:
        out += uv(v - previous)
        previous = v
    return bytes(out)


def main():
    rng = random.Random(SEED)
    dims = {"job": 4, "region": 3, "method": 4, "status": 4, "version": 2}
    families = [
        ("metricsbench_gauge_cpu_percent", 1, ["job", "region"], 400_000),
        ("metricsbench_requests_total", 1, ["job", "method", "status"], 300_000),
        ("metricsbench_request_duration_seconds", 10, ["job"], 150_000),
        ("metricsbench_latency_native", 1, ["job"], 100_000),
        ("metricsbench_build_info", 1, ["job", "version"], 50_000),
    ]
    series = []
    for name, per_instance, labels, count in families:
        product = 1
        for label in labels:
            product *= dims[label]
        for g in range(count // per_instance):
            pairs, stride = [], 1
            for label in labels:
                pairs.append((label, (g // stride) % dims[label]))
                stride *= dims[label]
            pairs.append(("instance", g // product))
            if per_instance == 10:
                for le in range(8):
                    series.append(([("__name__", name + "_bucket")] + pairs + [("le", le)]))
                series.append([("__name__", name + "_sum")] + pairs)
                series.append([("__name__", name + "_count")] + pairs)
            else:
                series.append([("__name__", name)] + pairs)
    skeys = [rng.getrandbits(64) for _ in series]
    postings = {}
    for skey, pairs in zip(skeys, series):
        for pair in pairs:
            postings.setdefault(pair, []).append(skey)
    print(
        f"series {len(series):,}, distinct pairs {len(postings):,}, "
        f"pairs per series {sum(map(len, postings.values())) / len(series):.2f}"
    )
    pair_key = {pair: rng.getrandbits(64) for pair in postings}
    label = Leaf(
        "series_label",
        sorted((pair_key[p], posting(sorted(v))) for p, v in postings.items()),
    )
    req = ("__name__", "metricsbench_requests_total")
    gauge = ("__name__", "metricsbench_gauge_cpu_percent")
    for pair in [req, gauge, ("job", 0), ("region", 0), ("method", 0), ("status", 0), ("instance", 0)]:
        print(f"  posting {pair}: {len(postings[pair]):,} series, frame of its bucket "
              f"{label.sizes[bucket(pair_key[pair], label.bits)]:,} B")

    def pass_a(plans):
        keys = {pair_key[p] for plan in plans for p in plan}
        return label.plan_bytes(keys)

    worked = [req, ("job", 0), ("method", 0), ("status", 0), ("instance", 0)]
    print("pass A, mb_single_counter_rate (k 5): d %d, %d B past the prefix" % pass_a([worked]))
    print("pass A, mb_selective_job_rate_sum (k 2): d %d, %d B past the prefix" % pass_a([[req, ("job", 0)]]))

    # A10 run 1: twelve plans gauge{job, region}, one per combination.
    plans = [[gauge, ("job", j), ("region", r)] for j in range(4) for r in range(3)]
    own = sorted(((pass_a([p])[1], i) for i, p in enumerate(plans)))
    print("A10 run 1: union K %d, union %s; each plan alone %d..%d B"
          % (len({x for p in plans for x in p}), "d %d, %d B" % pass_a(plans), own[0][0], own[-1][0]))
    admitted = []
    for _, i in own:
        if pass_a([plans[j] for j in admitted + [i]])[1] <= Q_A:
            admitted.append(i)
        else:
            break
    print("  admitted under Q_A in ascending own bytes: %d plans, union %s"
          % (len(admitted), "d %d, %d B" % pass_a([plans[j] for j in admitted])))

    # A10 run 2: sixty-four plans requests_total{instance=i}.
    plans = [[req, ("instance", i)] for i in range(64)]
    print("A10 run 2: union K %d, union d %d, %d B past the prefix; |S_i| %d each, union %d"
          % ((len({x for p in plans for x in p}),) + pass_a(plans)
             + (len(set(postings[req]) & set(postings[("instance", 0)])),
                len(set(postings[req]) & set().union(*(postings[("instance", i)] for i in range(64)))))))

    for classes in (21, 4096):
        sc = Leaf(f"series_class, {classes} classes",
                  sorted((k, uv(1) + uv(rng.randrange(classes) + 1)) for k in skeys))
        co_entries = []
        for c in range(classes):
            for e in range(c % E, E, classes):
                co_entries.append(((c ^ (1 << 63)), uv(e) + uv(1) + uv(1)))
        co = Leaf(f"class_objects, {classes} classes", sorted(co_entries))
        share = Q_BC - co.body
        print(f"  pass B share of Q_BC: {Q_BC:,} - {co.body:,} = {share:,} B past the prefixes")
        for m in (1, 64, 128, 192, 256, 320, 512, 1024, 4096, 75_000):
            trials = 2_000 if m in (192, 256, 320) else (20 if m == 75_000 else 200)
            ds, bs = [], []
            for _ in range(trials):
                d, b = sc.plan_bytes(rng.sample(skeys, m))
                ds.append(d)
                bs.append(b)
            over = sum(b > share for b in bs)
            print(f"  M {m:>6,}: d {sum(ds) / trials:,.0f}, pass B {sum(bs) / trials:,.0f} B mean, "
                  f"max {max(bs):,}, over share in {over} of {trials}")
        lo = 64
        while True:
            bs = [sc.plan_bytes(rng.sample(skeys, lo + 8))[1] for _ in range(400)]
            if sum(bs) / len(bs) > share:
                print(f"  mean crosses the share between M {lo} and {lo + 8}")
                break
            lo += 8


if __name__ == "__main__":
    main()
