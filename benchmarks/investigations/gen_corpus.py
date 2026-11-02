#!/usr/bin/env python3
"""Seeded generator of a correlated spans+logs corpus for the selective
investigations harness (issue #3699, ADR-3692 decision 8).

Writes, under --out:
  spans.jsonl, logs.jsonl        canonical row data (always)
  spans.parquet, logs.parquet    same data, Parquet (only when pyarrow is
                                  importable; the real box lanes install it,
                                  the kit.test.sh environment does not need
                                  to -- see README.md "Why JSONL and Parquet")
  spans.mapping.toml             ravel-cli load --signal spans mapping
  logs.mapping.toml              ravel-cli load --signal logs mapping
  truth.json                     every planted key/needle and its expected
                                  answer, consumed by evaluate.py and by
                                  statements.toml's truth_key references

Determinism: everything is derived from --seed through one
random.Random instance; no wall clock, no os.urandom.

Block geometry assumption: this generator plants substring/needle
frequencies against its OWN documented row-block size (BLOCK_ROWS below),
not against whatever block size the real ingest/segment writer picks for
a given corpus. See README.md "Block geometry assumption" for why an
exact match is not attempted and why tests tolerate 20% slack.
"""
import argparse
import json
import math
import os
import random
import sys

BLOCK_ROWS = 256  # documented generator-side block-geometry assumption

SERVICES = [f"svc-{i:02d}" for i in range(20)]
SPAN_NAMES = [f"op-{i:02d}" for i in range(50)]
BODY_TEMPLATES = [
    "request {rid} processed in {dur}ms",
    "handled {rid} for {svc} status=ok",
    "cache miss for key {rid}",
    "retrying {rid} after backoff",
    "finished batch {rid} with {dur} rows",
]
TAG_NEEDLES = [
    ("needle-rare-1blk", "1_block"),
    ("needle-sparse-0p1pct", "0.1pct"),
    ("needle-common-10pct", "10pct"),
]
REQUEST_ID_PRESENT = "req-planted-present-0001"
REQUEST_ID_ABSENT = "req-planted-absent-0001"


def hex_id(rng, nbytes):
    return "".join(rng.choice("0123456789abcdef") for _ in range(nbytes * 2))


def block_plan(rng, base_rows):
    """Choose, for each TAG_NEEDLES frequency, a disjoint set of block
    indices over range(ceil(base_rows / BLOCK_ROWS)), and the exact row
    count those blocks cover (the last block may be partial)."""
    n_blocks = max(1, math.ceil(base_rows / BLOCK_ROWS))
    pool = list(range(n_blocks))
    rng.shuffle(pool)
    plan = {}
    used = set()
    fractions = {"1_block": None, "0.1pct": 0.001, "10pct": 0.10}
    for value, freq in TAG_NEEDLES:
        if freq == "1_block":
            want = 1
        else:
            want = max(1, round(fractions[freq] * n_blocks))
        chosen = []
        for b in pool:
            if b in used:
                continue
            chosen.append(b)
            used.add(b)
            if len(chosen) == want:
                break
        rows = 0
        for b in chosen:
            start = b * BLOCK_ROWS
            end = min(start + BLOCK_ROWS, base_rows)
            rows += max(0, end - start)
        plan[value] = {
            "frequency": freq,
            "blocks": sorted(chosen),
            "n_blocks_total": n_blocks,
            "expected_count": rows,
        }
    return plan


def row_in_blocks(row_index, blocks):
    b = row_index // BLOCK_ROWS
    return b in blocks


def gen_traces(rng, n_traces, hours, failing_service, failing_hour, failing_count):
    """Return list of trace dicts: trace_id, service, hour, start_ns, error."""
    traces = []
    hour_ns = 3600 * 1_000_000_000
    for i in range(n_traces):
        hour = (i * hours) // n_traces if n_traces else 0
        hour = min(hour, hours - 1)
        service = SERVICES[rng.randrange(len(SERVICES))]
        start_ns = hour * hour_ns + rng.randrange(hour_ns)
        traces.append(
            {
                "trace_id": hex_id(rng, 16),
                "service": service,
                "hour": hour,
                "start_ns": start_ns,
                "error": rng.random() < 0.02,
            }
        )
    # Force exactly `failing_count` traces into the planted failing cell,
    # overriding their natural service/hour/error assignment.
    forced = rng.sample(range(n_traces), min(failing_count, n_traces))
    for i in forced:
        traces[i]["service"] = failing_service
        traces[i]["hour"] = failing_hour
        traces[i]["error"] = True
    return traces, [traces[i]["trace_id"] for i in forced]


def write_spans(fh_jsonl, traces, spans_per_trace, rng):
    count = 0
    for tr in traces:
        span_ids = []
        for s in range(spans_per_trace):
            span_id = hex_id(rng, 8)
            span_ids.append(span_id)
            parent = None if s == 0 else rng.choice(span_ids[:s])
            start_ns = tr["start_ns"] + (0 if s == 0 else rng.randrange(2_000_000_000))
            end_ns = start_ns + rng.randrange(1, 1_000_000_000)
            row = {
                "trace_id": tr["trace_id"],
                "span_id": span_id,
                "parent_span_id": parent,
                "name": SPAN_NAMES[rng.randrange(len(SPAN_NAMES))],
                "start_ts": start_ns,
                "end_ts": end_ns,
                "service": tr["service"],
                "status": "error" if (s == 0 and tr["error"]) else "ok",
            }
            fh_jsonl.write(json.dumps(row) + "\n")
            count += 1
    return count


def write_logs(fh_jsonl, traces, logs_per_trace, rng, plan, planted_present_index, noise=False):
    count = 0
    row_index = 0
    hour_ns = 3600 * 1_000_000_000
    present_written = False
    for ti, tr in enumerate(traces):
        for l in range(logs_per_trace):
            ts = tr["start_ns"] + rng.randrange(hour_ns // 24 + 1)
            dur = rng.randrange(1, 5000)
            tmpl = BODY_TEMPLATES[rng.randrange(len(BODY_TEMPLATES))]
            rid_background = f"req-bg-{tr['trace_id']}-{l:03d}"
            request_id = rid_background
            if (not noise) and planted_present_index == (ti, l):
                request_id = REQUEST_ID_PRESENT
                present_written = True
            tag = ""
            body_extra = ""
            if not noise:
                for value, _freq in TAG_NEEDLES:
                    if row_in_blocks(row_index, set(plan[value]["blocks"])):
                        tag = value
                        body_extra = " " + value
            body = tmpl.format(rid=request_id, dur=dur, svc=tr["service"]) + body_extra
            row = {
                "trace_id": tr["trace_id"],
                "service": tr["service"],
                "ts": ts,
                "body": body,
                "request_id": request_id,
                "tag": tag,
            }
            fh_jsonl.write(json.dumps(row) + "\n")
            count += 1
            row_index += 1
    return count, present_written


def maybe_write_parquet(jsonl_path, parquet_path):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        return False
    rows = []
    with open(jsonl_path) as fh:
        for line in fh:
            rows.append(json.loads(line))
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, parquet_path)
    return True


SPANS_MAPPING = """# Generated by gen_corpus.py -- ravel-cli load --signal spans mapping.
[spans]
trace_id_column       = "trace_id"
span_id_column        = "span_id"
parent_span_id_column = "parent_span_id"
name_column           = "name"
start_ts_column       = "start_ts"
start_ts_unit         = "nanos"
end_ts_column         = "end_ts"
end_ts_unit           = "nanos"
status_code_column    = "status"

[[spans.resource_attribute]]
key = "service.name"
column = "service"
type = "str"
"""

LOGS_MAPPING = """# Generated by gen_corpus.py -- ravel-cli load --signal logs mapping.
[logs]
ts_column        = "ts"
ts_unit          = "nanos"
body_column      = "body"
trace_id_column  = "trace_id"

[[logs.resource_attribute]]
key = "service.name"
column = "service"
type = "str"

[[logs.attribute]]
key = "request_id"
column = "request_id"
type = "str"

[[logs.attribute]]
key = "tag"
column = "tag"
type = "str"
"""


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--traces", type=int, required=True)
    ap.add_argument("--spans-per-trace", type=int, default=10)
    ap.add_argument("--logs-per-trace", type=int, required=True)
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--scale", type=int, choices=(1, 10, 100), default=1)
    args = ap.parse_args(argv)

    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)

    failing_service = SERVICES[0]
    failing_hour = args.hours // 2
    failing_count = min(50, args.traces)

    traces, failing_trace_ids = gen_traces(
        rng, args.traces, args.hours, failing_service, failing_hour, failing_count
    )

    base_logs_count = args.traces * args.logs_per_trace
    plan = block_plan(rng, base_logs_count)

    planted_present_index = (rng.randrange(args.traces), rng.randrange(args.logs_per_trace)) if args.traces and args.logs_per_trace else (0, 0)

    spans_path = os.path.join(args.out, "spans.jsonl")
    logs_path = os.path.join(args.out, "logs.jsonl")

    with open(spans_path, "w") as f_spans:
        spans_written = write_spans(f_spans, traces, args.spans_per_trace, rng)

    with open(logs_path, "w") as f_logs:
        logs_written, present_written = write_logs(
            f_logs, traces, args.logs_per_trace, rng, plan, planted_present_index
        )

    # Noise: (scale - 1) * base traces/spans/logs that match no planted
    # predicate. Hour/service are nudged off the failing cell; needles and
    # the planted request_id values are never emitted into noise rows.
    noise_traces_written = 0
    noise_spans_written = 0
    noise_logs_written = 0
    if args.scale > 1:
        n_noise = (args.scale - 1) * args.traces
        noise_traces, _ = gen_traces(rng, n_noise, args.hours, failing_service, failing_hour, 0)
        for tr in noise_traces:
            if tr["service"] == failing_service and tr["hour"] == failing_hour:
                tr["hour"] = (tr["hour"] + 1) % args.hours
        with open(spans_path, "a") as f_spans:
            noise_spans_written = write_spans(f_spans, noise_traces, args.spans_per_trace, rng)
        with open(logs_path, "a") as f_logs:
            noise_logs_written, _ = write_logs(
                f_logs, noise_traces, args.logs_per_trace, rng, plan, None, noise=True
            )
        noise_traces_written = len(noise_traces)

    spans_parquet_written = maybe_write_parquet(spans_path, os.path.join(args.out, "spans.parquet"))
    logs_parquet_written = maybe_write_parquet(logs_path, os.path.join(args.out, "logs.parquet"))

    with open(os.path.join(args.out, "spans.mapping.toml"), "w") as f:
        f.write(SPANS_MAPPING)
    with open(os.path.join(args.out, "logs.mapping.toml"), "w") as f:
        f.write(LOGS_MAPPING)

    # Saved trace ids for lookup arms: 40 distinct ids from the base
    # (non-noise) trace set, plus their start_ns/hour for window math.
    n_saved = min(40, args.traces)
    saved_idx = rng.sample(range(args.traces), n_saved)
    saved_trace_ids = [
        {"trace_id": traces[i]["trace_id"], "start_ns": traces[i]["start_ns"], "hour": traces[i]["hour"]}
        for i in saved_idx
    ]

    # IN-list fixtures over trace_id (spans+logs) and request_id (logs).
    def trace_id_list(n):
        n = min(n, args.traces)
        idx = rng.sample(range(args.traces), n)
        ids = [traces[i]["trace_id"] for i in idx]
        return {
            "trace_ids": ids,
            "expected_rows_spans": len(ids) * args.spans_per_trace,
            "expected_rows_logs": len(ids) * args.logs_per_trace,
        }

    in_lists_trace_id = {str(n): trace_id_list(n) for n in (1, 10, 100, 1000)}

    def request_id_list(n):
        total_rows = args.traces * args.logs_per_trace
        n = min(n, total_rows)
        chosen_trace_idx = [rng.randrange(args.traces) for _ in range(n)]
        ids = [f"req-bg-{traces[i]['trace_id']}-{rng.randrange(args.logs_per_trace):03d}" for i in chosen_trace_idx]
        # distinctness is not guaranteed by construction above; de-dup and
        # top up to n from the same pool rather than overclaim expected_rows.
        ids = list(dict.fromkeys(ids))[:n]
        return {"request_ids": ids, "expected_rows": len(ids)}

    in_lists_request_id = {str(n): request_id_list(n) for n in (1, 10, 100, 1000)}

    # Journey build-side tiers: trace ids for the join, capped to what the
    # corpus actually holds (documented: a smaller corpus caps the tier).
    def journey_tier(n):
        n = min(n, args.traces)
        idx = rng.sample(range(args.traces), n)
        ids = [traces[i]["trace_id"] for i in idx]
        return {
            "trace_ids": ids,
            "capped": n < (n if n == len(ids) else n),
            "requested": n,
            "actual": len(ids),
            "expected_rows": len(ids) * args.spans_per_trace * args.logs_per_trace,
        }

    journey_join = {str(n): journey_tier(n) for n in (1, 102, 1024, 10240)}

    truth = {
        "seed": args.seed,
        "traces": args.traces,
        "spans_per_trace": args.spans_per_trace,
        "logs_per_trace": args.logs_per_trace,
        "hours": args.hours,
        "scale": args.scale,
        "block_rows": BLOCK_ROWS,
        "base_logs_count": base_logs_count,
        "spans_written": spans_written + noise_spans_written,
        "logs_written": logs_written + noise_logs_written,
        "noise_traces_written": noise_traces_written,
        "spans_parquet_written": spans_parquet_written,
        "logs_parquet_written": logs_parquet_written,
        "failing_cell": {
            "service": failing_service,
            "hour": failing_hour,
            "trace_ids": sorted(failing_trace_ids),
            "count": len(failing_trace_ids),
        },
        "request_id": {
            "present": {"value": REQUEST_ID_PRESENT, "count": 1 if present_written else 0},
            "absent": {"value": REQUEST_ID_ABSENT, "count": 0},
        },
        "tag_needles": [
            {
                "value": value,
                "frequency": plan[value]["frequency"],
                "expected_count": plan[value]["expected_count"],
                "n_blocks_total": plan[value]["n_blocks_total"],
                "columns": ["tag", "body"],
            }
            for value, _ in TAG_NEEDLES
        ],
        "saved_trace_ids": saved_trace_ids,
        "in_lists": {
            "trace_id": in_lists_trace_id,
            "request_id": in_lists_request_id,
        },
        "journey_join": journey_join,
    }
    with open(os.path.join(args.out, "truth.json"), "w") as f:
        json.dump(truth, f, indent=2, sort_keys=True)
        f.write("\n")

    print(f"wrote {truth['spans_written']} spans, {truth['logs_written']} logs to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
