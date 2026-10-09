#!/usr/bin/env python3
"""Split a collapsed profile by operator: partial aggregate (the aggregate
runs inside RepartitionExec::pull_from_input's task), final aggregate,
RepartitionExec output coalescer, parquet decode, other.

usage: mode.py <collapsed.txt> <title>
"""
import sys
from collections import Counter

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from attr import ALLOC_MARK, PLUMBING, clean  # noqa: E402

KEYS = ("append_value", "allocator_api2", "next_buffer", "CountGroupsAccumulator",
        "ensure_capacity", "GroupValuesBytesView", "decode_page", "Accumulator", "GroupValueBuilder")
c = Counter()
for line in open(sys.argv[1]):
    stack, _, n = line.rstrip("\n").rpartition(" ")
    if not n.lstrip("-").isdigit():
        continue
    frames = [clean(f) for f in stack.split(";")]
    cut = next((i for i, f in enumerate(frames) if ALLOC_MARK.search(f)), len(frames))
    rest = [f for f in reversed(frames[:cut]) if not PLUMBING.match(f)]
    site = rest[0] if rest else "?"
    site = next((k for k in KEYS if k in site), "other: " + site[:80])
    whole = ";".join(frames)
    # Task type names embed `pull_from_input` as a generic parameter, so only
    # frames that start with the driver function count, and the nearest one
    # above the innermost aggregate decides: the partial aggregate runs inside
    # the hash RepartitionExec's pull_from_input task, the final aggregate
    # inside a RecordBatchReceiverStream's run_input task.
    agg = [i for i, f in enumerate(frames) if f.startswith("<datafusion_physical_plan::aggregates::row_hash::GroupedHashAggregateStream")]
    if agg:
        drivers = [f for f in frames[: agg[-1]]
                   if f.startswith("<datafusion_physical_plan::repartition::RepartitionExec>::pull_from_input")
                   or f.startswith("<datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder>::run_input")]
        last = drivers[-1] if drivers else ""
        mode = ("partial aggregate" if "pull_from_input" in last
                else "final aggregate" if "run_input" in last else "aggregate (driver unknown)")
    elif "repartition" in whole:
        mode = "repartition output coalescer"
    elif "parquet" in whole:
        mode = "parquet scan"
    else:
        mode = "other"
    c[(mode, site)] += int(n)
tot = sum(c.values())
print(f"# {sys.argv[2]}, total {tot} B")
for (m, s), b in sorted(c.items(), key=lambda kv: -kv[1]):
    if abs(b) >= 1_000_000:
        print(f"{b:>12} {100*b/tot:5.1f}%  {m:30s} {s}")
