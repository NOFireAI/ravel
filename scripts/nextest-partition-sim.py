#!/usr/bin/env python3
"""Replay and simulate cargo-nextest's `--partition hash:M/N` split.

nextest assigns a test to leg `xxh64(test_name, seed 0) % N`, where the name
is the test's name inside its binary, without the binary id (nextest-runner
`partition.rs`, fixed for the 0.9 series). CI pins the nextest version, and
a bump of that pin could change the rule, so `replay` checks it against the
run being measured before anything is sized on it.

Input: one nextest log per leg, in leg order: a CI job log as downloaded
(`gh api --allow-escape-sequences repos/O/R/actions/jobs/ID/logs`), or any
output holding nextest's `PASS [ 1.234s] (k/n) binary test` lines. ANSI
codes and a leading timestamp are stripped.

  hash NAME...                     print xxh64(NAME, 0) in hex
  replay LEG1.log ... LEGN.log     exit 1 if any test ran on a leg the
                                   rule does not give it
  simulate --legs 4,5 LOG...       summed test seconds per leg at each count,
                                   with the slowest leg's estimated wall time
                                   (summed seconds over --parallelism)
  simulate ... --band N:LO:HI      also exit 1 when the slowest leg's summed
                                   seconds at N legs fall outside [LO, HI]

Exit codes: 0 ok, 1 a misassignment or a band miss, 2 a leg log with no
result line or an unreadable one, 64 usage.
"""

import argparse
import re
import sys

M64 = (1 << 64) - 1
P1 = 11400714785074694791
P2 = 14029467366897019727
P3 = 1609587929392839161
P4 = 9650029242287828579
P5 = 2870177450012600261


def _rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & M64


def _round(acc, lane):
    acc = (acc + lane * P2) & M64
    return (_rotl(acc, 31) * P1) & M64


def _merge(acc, val):
    acc ^= _round(0, val)
    return (acc * P1 + P4) & M64


def xxh64(data, seed=0):
    """XXH64 of `data` (bytes), as nextest's `xxh64(name.as_bytes(), 0)`."""
    n = len(data)
    i = 0
    if n >= 32:
        v1 = (seed + P1 + P2) & M64
        v2 = (seed + P2) & M64
        v3 = seed
        v4 = (seed - P1) & M64
        while i + 32 <= n:
            v1 = _round(v1, int.from_bytes(data[i:i + 8], "little"))
            v2 = _round(v2, int.from_bytes(data[i + 8:i + 16], "little"))
            v3 = _round(v3, int.from_bytes(data[i + 16:i + 24], "little"))
            v4 = _round(v4, int.from_bytes(data[i + 24:i + 32], "little"))
            i += 32
        h = (_rotl(v1, 1) + _rotl(v2, 7) + _rotl(v3, 12) + _rotl(v4, 18)) & M64
        for v in (v1, v2, v3, v4):
            h = _merge(h, v)
    else:
        h = (seed + P5) & M64
    h = (h + n) & M64
    while i + 8 <= n:
        h ^= _round(0, int.from_bytes(data[i:i + 8], "little"))
        h = (_rotl(h, 27) * P1 + P4) & M64
        i += 8
    if i + 4 <= n:
        h ^= (int.from_bytes(data[i:i + 4], "little") * P1) & M64
        h = (_rotl(h, 23) * P2 + P3) & M64
        i += 4
    while i < n:
        h ^= (data[i] * P5) & M64
        h = (_rotl(h, 11) * P1) & M64
        i += 1
    h ^= h >> 33
    h = (h * P2) & M64
    h ^= h >> 29
    h = (h * P3) & M64
    h ^= h >> 32
    return h


ANSI = re.compile(r"\x1b\[[0-9;]*m")
# `PASS [   1.234s] (  12/4102) ravel-sql::esql stack::the_widest...`; the
# binary id never contains a space, the test name follows it.
# Any terminal status nextest prints with a duration and a `(k/n)` count:
# PASS, FAIL, TIMEOUT, SIGSEGV, LEAK, ABORT and the like, so a test that
# timed out or crashed still counts on its leg.
RESULT = re.compile(
    r"\b([A-Z][A-Z0-9-]+)\s+\[\s*([0-9.]+)s\]\s+\(\s*\d+/\d+\)\s+(\S+)\s+(\S.*?)\s*$"
)


def read_leg(path):
    """[(binary, name, seconds)] from one leg's log. A `SLOW [> 60.000s]`
    progress line carries no `(k/n)` count and does not match; a test that
    appears more than once (a retry) keeps its last duration."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError as err:
        print(f"nextest-partition-sim: cannot read {path}: {err}", file=sys.stderr)
        sys.exit(2)
    tests = {}
    for line in text.splitlines():
        m = RESULT.search(ANSI.sub("", line))
        if m:
            _, secs, binary, name = m.groups()
            tests[(binary, name)] = float(secs)
    return [(b, n, s) for (b, n), s in tests.items()]


def read_legs(paths):
    """Every leg's tests; a leg whose log holds no result line (missing,
    truncated, or the wrong job) stops the run rather than passing as a leg
    with nothing on it."""
    legs = [read_leg(p) for p in paths]
    empty = [p for p, tests in zip(paths, legs) if not tests]
    if empty:
        for p in empty:
            print(f"nextest-partition-sim: no nextest result lines in {p}", file=sys.stderr)
        sys.exit(2)
    return legs


def cmd_hash(args):
    for name in args.names:
        print(f"{xxh64(name.encode()):016x}  {name}")
    return 0


def cmd_replay(args):
    legs = read_legs(args.logs)
    n = len(legs)
    wrong = []
    count = 0
    for leg, tests in enumerate(legs):
        for binary, name, _ in tests:
            count += 1
            want = xxh64(name.encode()) % n
            if want != leg:
                wrong.append((binary, name, leg + 1, want + 1))
    print(f"replayed {count} tests over {n} legs: {len(wrong)} misassigned")
    for binary, name, got, want in wrong[:20]:
        print(f"  {binary} {name}: ran on leg {got}, the rule gives leg {want}")
    return 1 if wrong else 0


def parse_band(text):
    try:
        legs, lo, hi = text.split(":")
        return int(legs), float(lo), float(hi)
    except ValueError:
        print(f"nextest-partition-sim: --band wants N:LO:HI, got {text!r}", file=sys.stderr)
        sys.exit(64)


def cmd_simulate(args):
    tests = [t for leg in read_legs(args.logs) for t in leg]
    try:
        counts = [int(c) for c in args.legs.split(",")]
    except ValueError:
        print(f"nextest-partition-sim: --legs wants a list like 4,5, got {args.legs!r}", file=sys.stderr)
        return 64
    if not counts or min(counts) < 1:
        print("nextest-partition-sim: --legs wants positive counts", file=sys.stderr)
        return 64
    if args.parallelism <= 0:
        print("nextest-partition-sim: --parallelism wants a positive number", file=sys.stderr)
        return 64
    bands = [parse_band(b) for b in args.band]
    slowest = {}
    print(f"{len(tests)} tests, {sum(s for _, _, s in tests):.0f} summed seconds")
    for n in counts:
        sums = [0.0] * n
        for _, name, secs in tests:
            sums[xxh64(name.encode()) % n] += secs
        slowest[n] = max(sums)
        legs = " ".join(f"{s:.0f}" for s in sums)
        print(
            f"legs={n} slowest={slowest[n]:.0f}s est_wall={slowest[n] / args.parallelism:.0f}s sums=[{legs}]"
        )
    rc = 0
    for n, lo, hi in bands:
        if n not in slowest:
            print(f"nextest-partition-sim: --band {n} names a leg count --legs does not", file=sys.stderr)
            return 64
        ok = lo <= slowest[n] <= hi
        print(f"band legs={n} [{lo:.0f}, {hi:.0f}]: slowest {slowest[n]:.0f}s {'inside' if ok else 'OUTSIDE'}")
        if not ok:
            rc = 1
    return rc


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("hash")
    p.add_argument("names", nargs="+")
    p.set_defaults(fn=cmd_hash)
    p = sub.add_parser("replay")
    p.add_argument("logs", nargs="+")
    p.set_defaults(fn=cmd_replay)
    p = sub.add_parser("simulate")
    p.add_argument("--legs", required=True)
    p.add_argument("--band", action="append", default=[])
    p.add_argument("--parallelism", type=float, default=3.8)
    p.add_argument("logs", nargs="+")
    p.set_defaults(fn=cmd_simulate)
    try:
        args = parser.parse_args()
    except SystemExit as exc:
        sys.exit(64 if exc.code else 0)
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
