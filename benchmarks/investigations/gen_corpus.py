#!/usr/bin/env python3
"""Seeded generator of a correlated spans+logs corpus for the selective
investigations harness (issue #3699, ADR-3692 decision 8).

Writes, under --out:
  spans.jsonl, logs.jsonl        row data (--format jsonl or both)
  spans.parquet, logs.parquet    the same rows as Parquet (--format parquet
                                  or both; needs pyarrow)
  spans.mapping.toml             ravel-cli load --signal spans mapping
  logs.mapping.toml              ravel-cli load --signal logs mapping
  truth.json                     every planted key and its exact answer

Determinism: every value derives from --seed. The base corpus draws from one
random.Random(seed); the scale noise draws from a second generator seeded
from (seed, "noise"), so the base rows are byte-identical at every --scale.

Time: every event-time hour is a function of the timestamp,
hour = ts // 3600e9 - start_hour. A trace starts at least TRACE_MARGIN_NS
before its hour ends, and every span and log of it lands before that end, so
a trace and all of its rows share one hour.
"""
import argparse
import json
import math
import os
import random
import sys

HOUR_NS = 3600 * 1_000_000_000
TRACE_MARGIN_NS = 120 * 1_000_000_000
CHILD_START_MAX_NS = 2_000_000_000
SPAN_DUR_MAX_NS = 1_000_000_000
LOG_OFFSET_MAX_NS = 60 * 1_000_000_000
# The rlog writer's block target: docs/log-segment-format.md:438,
# crates/ravel-logseg/src/writer.rs:99.
BLOCK_ROWS = 8192
DEFAULT_START_HOUR = 1767225600 // 3600  # 2026-01-01T00:00Z

SERVICES = [f"svc-{i:02d}" for i in range(20)]
FAILING_SERVICE = 0
FAILING_CELL_TRACES = 50
SPAN_NAMES = [f"op-{i:02d}" for i in range(50)]
BODY_TEMPLATES = [
    "request {rid} processed in {dur}ms",
    "handled {rid} for {svc} status ok",
    "cache miss for key {rid}",
    "retrying {rid} after backoff",
    "finished batch {rid} with {dur} rows",
]
NEEDLE_RARE = "needlerare"
NEEDLE_SPARSE = "needlesparse"
NEEDLE_COMMON = "needlecommon"
REQUEST_ID_PRESENT = "req-planted-present-0001"
REQUEST_ID_ABSENT = "req-planted-absent-0001"
REQUEST_ID_TAIL = "req-planted-tail-0001"
SAVED_TRACES = 40
IN_SIZES = (1, 10, 100, 1000)
JOURNEY_TIERS = (1, 102, 1024, 10240)
ROW_SET_MAX = 100

# OTLP status codes.
STATUS_UNSET, STATUS_OK, STATUS_ERROR = 0, 1, 2


def hex_of(value, nbytes):
    return f"{value:0{nbytes * 2}x}"


class Trace:
    __slots__ = ("trace_id", "start_ns", "service", "error")

    def __init__(self, trace_id, start_ns, service, error):
        self.trace_id = trace_id
        self.start_ns = start_ns
        self.service = service
        self.error = error


def hour_of(ts_ns, start_hour):
    return ts_ns // HOUR_NS - start_hour


def draw_traces(rng, n, hours, start_hour, seen_ids):
    traces = []
    base_ns = start_hour * HOUR_NS
    for _ in range(n):
        tid = rng.getrandbits(128)
        while tid in seen_ids or tid == 0:
            tid = rng.getrandbits(128)
        seen_ids.add(tid)
        hour = rng.randrange(hours)
        start = base_ns + hour * HOUR_NS + rng.randrange(HOUR_NS - TRACE_MARGIN_NS)
        traces.append(Trace(tid, start, rng.randrange(len(SERVICES)), rng.random() < 0.02))
    traces.sort(key=lambda t: (t.start_ns, t.trace_id))
    return traces


def needle_block_counts(n_rows):
    """Blocks each needle tier touches: rare 1, sparse 0.1 % of the
    generator's BLOCK_ROWS-row blocks (at least 2), common 10 % (at least one
    more than sparse), so single < sparse < common whenever the corpus has
    that many blocks."""
    n_blocks = math.ceil(n_rows / BLOCK_ROWS)
    sparse = min(max(2, round(0.001 * n_blocks)), n_blocks)
    common = min(max(sparse + 1, round(0.10 * n_blocks)), n_blocks)
    return {NEEDLE_RARE: min(1, n_blocks), NEEDLE_SPARSE: sparse, NEEDLE_COMMON: common}


def plan_needles(rng, n_rows):
    """Row indices for the three needle tiers: one row in each of the tier's
    blocks (needle_block_counts), drawn independently per tier, no row
    carrying two needles."""
    out = {NEEDLE_RARE: set(), NEEDLE_SPARSE: set(), NEEDLE_COMMON: set()}
    if n_rows < BLOCK_ROWS:
        return out
    n_blocks = math.ceil(n_rows / BLOCK_ROWS)
    used = set()
    for value, k in needle_block_counts(n_rows).items():
        for b in sorted(rng.sample(range(n_blocks), k)):
            lo, hi = b * BLOCK_ROWS, min((b + 1) * BLOCK_ROWS, n_rows)
            r = rng.randrange(lo, hi)
            while r in used:
                r = rng.randrange(lo, hi)
            used.add(r)
            out[value].add(r)
    return out


class Sink:
    """Writes rows as JSONL and/or Parquet, in batches."""

    def __init__(self, out, name, schema_fields, fmt):
        self.jsonl = open(os.path.join(out, f"{name}.jsonl"), "w") if fmt in ("jsonl", "both") else None
        self.writer = None
        self.buf = []
        self.count = 0
        if fmt in ("parquet", "both"):
            import pyarrow as pa
            import pyarrow.parquet as pq

            self.pa = pa
            self.schema = pa.schema(schema_fields(pa))
            self.writer = pq.ParquetWriter(os.path.join(out, f"{name}.parquet"), self.schema)

    def add(self, row):
        self.count += 1
        if self.jsonl is not None:
            self.jsonl.write(json.dumps(row, separators=(",", ":")) + "\n")
        if self.writer is not None:
            self.buf.append(row)
            if len(self.buf) >= 200_000:
                self.flush()

    def flush(self):
        if self.writer is not None and self.buf:
            self.writer.write_table(self.pa.Table.from_pylist(self.buf, schema=self.schema))
            self.buf = []

    def close(self):
        self.flush()
        if self.writer is not None:
            self.writer.close()
        if self.jsonl is not None:
            self.jsonl.close()


def spans_schema(pa):
    return [
        ("trace_id", pa.string()),
        ("span_id", pa.string()),
        ("parent_span_id", pa.string()),
        ("name", pa.string()),
        ("start_ts", pa.int64()),
        ("end_ts", pa.int64()),
        ("service", pa.string()),
        ("status_code", pa.int32()),
    ]


def logs_schema(pa):
    return [
        ("ts", pa.int64()),
        ("body", pa.string()),
        ("trace_id", pa.string()),
        ("service", pa.string()),
        ("request_id", pa.string()),
        ("tag", pa.string()),
        ("needle_rare", pa.string()),
    ]


def emit_trace(rng, tr, spans_per_trace, spans_sink, saved_spans):
    tid = hex_of(tr.trace_id, 16)
    span_ids = []
    for s in range(spans_per_trace):
        sid = hex_of(rng.getrandbits(64) | 1, 8)
        while sid in span_ids:
            sid = hex_of(rng.getrandbits(64) | 1, 8)
        span_ids.append(sid)
        parent = None if s == 0 else span_ids[rng.randrange(s)]
        start = tr.start_ns if s == 0 else tr.start_ns + rng.randrange(1, CHILD_START_MAX_NS)
        end = start + rng.randrange(1, SPAN_DUR_MAX_NS)
        if s == 0:
            code = STATUS_ERROR if tr.error else STATUS_OK
        else:
            code = STATUS_UNSET
        spans_sink.add(
            {
                "trace_id": tid,
                "span_id": sid,
                "parent_span_id": parent,
                "name": SPAN_NAMES[rng.randrange(len(SPAN_NAMES))],
                "start_ts": start,
                "end_ts": end,
                "service": SERVICES[tr.service],
                "status_code": code,
            }
        )
    if saved_spans is not None:
        saved_spans[tid] = sorted(span_ids)
    return tid


def request_id_of(tid, l):
    return f"req-{tid[:16]}-{l:03d}"


def emit_logs(rng, tr, tid, logs_per_trace, logs_sink, row_base, needle_of, present_row, present_id=REQUEST_ID_PRESENT,
              needle_at=None):
    for l in range(logs_per_trace):
        r = row_base + l
        ts = tr.start_ns + rng.randrange(LOG_OFFSET_MAX_NS)
        dur = rng.randrange(1, 5000)
        rid = present_id if r == present_row else request_id_of(tid, l)
        body = BODY_TEMPLATES[rng.randrange(len(BODY_TEMPLATES))].format(rid=rid, dur=dur, svc=SERVICES[tr.service])
        tag = needle_of.get(r)
        if tag is not None:
            body = f"{body} {tag}"
            needle_at[tag].add(logs_sink.count)
        logs_sink.add(
            {
                "ts": ts,
                "body": body,
                "trace_id": tid,
                "service": SERVICES[tr.service],
                "request_id": rid,
                "tag": tag,
                "needle_rare": tag if tag == NEEDLE_RARE else None,
            }
        )


def answer(rows, row_set=None, value=None, requested=None):
    out = {"rows": rows}
    if row_set is not None and len(row_set) <= ROW_SET_MAX:
        out["row_set"] = sorted(row_set)
    if value is not None:
        out["value"] = value
    if requested is not None:
        out["requested"] = requested
        out["capped"] = rows_requested_capped(requested, value if value is not None else rows)
    return out


def rows_requested_capped(requested, got):
    return got < requested


def mapping_texts():
    spans = """# Generated by gen_corpus.py: ravel-cli load --signal spans mapping.
[spans]
trace_id_column       = "trace_id"
span_id_column        = "span_id"
parent_span_id_column = "parent_span_id"
name_column           = "name"
start_ts_column       = "start_ts"
start_ts_unit         = "nanos"
end_ts_column         = "end_ts"
end_ts_unit           = "nanos"
status_code_column    = "status_code"

[[spans.resource_attribute]]
key = "service.name"
column = "service"
type = "str"
"""
    logs = """# Generated by gen_corpus.py: ravel-cli load --signal logs mapping.
[logs]
ts_column       = "ts"
ts_unit         = "nanos"
body_column     = "body"
trace_id_column = "trace_id"

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

[[logs.attribute]]
key = "needle_rare"
column = "needle_rare"
type = "str"
"""
    return spans, logs


def resolve_format(fmt):
    if fmt != "auto":
        return fmt
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return "jsonl"
    return "parquet"


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--traces", type=int, default=20_000_000)
    ap.add_argument("--spans-per-trace", type=int, default=10)
    ap.add_argument("--logs-per-trace", type=int, default=5)
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--seed", type=int, default=3692)
    ap.add_argument("--out", required=True)
    ap.add_argument("--scale", type=int, choices=(1, 10, 100), default=1)
    ap.add_argument("--start-hour", type=int, default=DEFAULT_START_HOUR, help="first event hour, in hours since the Unix epoch")
    ap.add_argument("--format", choices=("auto", "jsonl", "parquet", "both"), default="auto")
    ap.add_argument(
        "--noise-only",
        action="store_true",
        help="write unplanted rows only; truth.json lists no planted keys",
    )
    ap.add_argument(
        "--tail",
        action="store_true",
        help=f"the mixed state's extra hour: planted like the base corpus, with {REQUEST_ID_TAIL} as its present request id",
    )
    args = ap.parse_args(argv)
    if args.traces < 1 or args.hours < 1 or args.spans_per_trace < 1 or args.logs_per_trace < 1:
        ap.error("--traces, --hours, --spans-per-trace and --logs-per-trace must be positive")
    if args.tail and args.noise_only:
        ap.error("--tail plants keys; --noise-only plants none")
    present_id = REQUEST_ID_TAIL if args.tail else REQUEST_ID_PRESENT
    fmt = resolve_format(args.format)
    os.makedirs(args.out, exist_ok=True)

    rng = random.Random(args.seed)
    seen = set()
    base = draw_traces(rng, args.traces, args.hours, args.start_hour, seen)
    failing_hour = args.hours // 2
    hour_start_ns = (args.start_hour + failing_hour) * HOUR_NS
    hour_end_ns = hour_start_ns + HOUR_NS
    in_hour = [i for i, t in enumerate(base) if hour_of(t.start_ns, args.start_hour) == failing_hour]

    planted = []
    if not args.noise_only:
        # The failing cell holds exactly the planted traces: no background
        # trace of the failing service errors in the failing hour.
        for t in base:
            if t.service == FAILING_SERVICE and hour_of(t.start_ns, args.start_hour) == failing_hour:
                t.error = False
        planted = sorted(rng.sample(in_hour, min(FAILING_CELL_TRACES, len(in_hour))))
        for i in planted:
            base[i].service = FAILING_SERVICE
            base[i].error = True

    n_log_rows = args.traces * args.logs_per_trace
    needles = plan_needles(rng, n_log_rows) if not args.noise_only else {NEEDLE_RARE: set(), NEEDLE_SPARSE: set(), NEEDLE_COMMON: set()}
    needle_of = {}
    for value, rows in needles.items():
        for r in rows:
            needle_of[r] = value
    present_row = rng.randrange(n_log_rows) if not args.noise_only else -1
    saved_idx = sorted(rng.sample(range(args.traces), min(SAVED_TRACES, args.traces)))
    saved_set = set(saved_idx)

    noise = []
    if args.scale > 1:
        nrng = random.Random(f"{args.seed}:noise")
        noise = draw_traces(nrng, (args.scale - 1) * args.traces, args.hours, args.start_hour, seen)
        for t in noise:
            # Off the failing cell by timestamp: the hour is the trace's own.
            if t.service == FAILING_SERVICE and hour_of(t.start_ns, args.start_hour) == failing_hour:
                t.service = FAILING_SERVICE + 1
    else:
        nrng = None

    spans_sink = Sink(args.out, "spans", spans_schema, fmt)
    logs_sink = Sink(args.out, "logs", logs_schema, fmt)
    saved_spans = {}
    tids = []
    # Needle rows by their position in the written file, which is what a
    # BLOCK_ROWS block boundary cuts once noise rows are interleaved.
    needle_at = {v: set() for v in needles}
    # Interleave base and noise by start so each file is sorted by trace start.
    bi = ni = 0
    while bi < len(base) or ni < len(noise):
        take_base = ni >= len(noise) or (bi < len(base) and (base[bi].start_ns, base[bi].trace_id) <= (noise[ni].start_ns, noise[ni].trace_id))
        if take_base:
            tr = base[bi]
            tid = emit_trace(rng, tr, args.spans_per_trace, spans_sink, saved_spans if bi in saved_set else None)
            emit_logs(rng, tr, tid, args.logs_per_trace, logs_sink, bi * args.logs_per_trace, needle_of, present_row,
                      present_id, needle_at)
            tids.append(tid)
            bi += 1
        else:
            tr = noise[ni]
            tid = emit_trace(nrng, tr, args.spans_per_trace, spans_sink, None)
            emit_logs(nrng, tr, tid, args.logs_per_trace, logs_sink, -1 - args.logs_per_trace, {}, None)
            ni += 1
    spans_sink.close()
    logs_sink.close()

    spans_map, logs_map = mapping_texts()
    with open(os.path.join(args.out, "spans.mapping.toml"), "w") as f:
        f.write(spans_map)
    with open(os.path.join(args.out, "logs.mapping.toml"), "w") as f:
        f.write(logs_map)

    spt, lpt = args.spans_per_trace, args.logs_per_trace
    answers = {}
    saved = []
    for n, i in enumerate(saved_idx):
        tid = tids[i]
        saved.append({"trace_id": tid, "start_ns": base[i].start_ns, "hour": hour_of(base[i].start_ns, args.start_hour)})
        answers[f"saved_trace.{n}.spans"] = answer(spt, row_set=saved_spans[tid])
        answers[f"saved_trace.{n}.logs"] = answer(lpt)
    failing_ids = sorted(tids[i] for i in planted)
    answers["failing_cell.trace_ids"] = answer(len(failing_ids), row_set=failing_ids)
    answers["failing_cell.logs_count"] = answer(1, value=len(failing_ids) * lpt)

    in_lists = {"trace_id": {}, "request_id": {}}
    drawn = {}
    for size in IN_SIZES:
        got = min(size, args.traces)
        drawn[("trace_id", size)] = rng.sample(range(args.traces), got)
        ids = sorted(tids[i] for i in drawn[("trace_id", size)])
        in_lists["trace_id"][str(size)] = ids
        answers[f"in_trace_id.{size}.spans"] = answer(got * spt, requested=size * spt)
        answers[f"in_trace_id.{size}.logs"] = answer(got * lpt, requested=size * lpt)
    pool = [r for r in range(n_log_rows) if r != present_row]
    for size in IN_SIZES:
        got = min(size, len(pool))
        rows = rng.sample(pool, got)
        drawn[("request_id", size)] = rows
        ids = sorted(request_id_of(tids[r // lpt], r % lpt) for r in rows)
        in_lists["request_id"][str(size)] = ids
        answers[f"in_request_id.{size}"] = answer(got, row_set=ids, requested=size)

    # The 1,001-key controls: each 1000 list plus one key it does not hold,
    # or None when every key is already in it.
    top = IN_SIZES[-1]
    control_extra = {"trace_id": None, "request_id": None}
    taken = set(drawn[("trace_id", top)])
    if len(taken) < args.traces:
        i = rng.randrange(args.traces)
        while i in taken:
            i = rng.randrange(args.traces)
        control_extra["trace_id"] = tids[i]
    taken = set(drawn[("request_id", top)])
    if len(taken) < len(pool):
        r = pool[rng.randrange(len(pool))]
        while r in taken:
            r = pool[rng.randrange(len(pool))]
        control_extra["request_id"] = request_id_of(tids[r // lpt], r % lpt)
    got = len(in_lists["trace_id"][str(top)]) + (control_extra["trace_id"] is not None)
    answers[f"in_trace_id.{top + 1}.spans"] = answer(got * spt, requested=(top + 1) * spt)
    answers[f"in_trace_id.{top + 1}.logs"] = answer(got * lpt, requested=(top + 1) * lpt)
    got = len(in_lists["request_id"][str(top)]) + (control_extra["request_id"] is not None)
    answers[f"in_request_id.{top + 1}"] = answer(got, requested=top + 1)

    if not args.noise_only:
        present_tid = tids[present_row // lpt]
        answers["request_id.present"] = answer(1, row_set=[present_tid])
    else:
        answers["request_id.present"] = answer(0, row_set=[])
    answers["request_id.absent"] = answer(0, row_set=[])
    for value, rows in needles.items():
        answers[f"needle.{value}"] = answer(1, value=len(rows))
    needle_blocks = {v: len({r // BLOCK_ROWS for r in rows}) for v, rows in needle_at.items()}
    needle_blocks["any"] = len({r // BLOCK_ROWS for rows in needle_at.values() for r in rows})

    # The erasure_in_flight state's victim: one log row no statement reads, so
    # a pending erasure leaves every answer as truth.json gives it.
    victim = None
    if not args.noise_only:
        busy_traces = set(saved_idx) | set(planted)
        busy_traces |= {i for i, t in enumerate(base) if hour_of(t.start_ns, args.start_hour) == failing_hour}
        busy_tids = {tids[i] for i in busy_traces} | {t for ids in in_lists["trace_id"].values() for t in ids}
        busy_tids.add(control_extra["trace_id"])
        busy_rids = {r for ids in in_lists["request_id"].values() for r in ids}
        busy_rids.add(control_extra["request_id"])
        for r in range(n_log_rows):
            tid = tids[r // lpt]
            rid = request_id_of(tid, r % lpt)
            if r != present_row and r not in needle_of and tid not in busy_tids and rid not in busy_rids:
                victim = {"request_id": rid, "trace_id": tid}
                answers["erasure_victim.present"] = answer(1, row_set=[tid])
                answers["erasure_victim.erased"] = answer(0, row_set=[])
                break

    # Journey C build sides: the first k root spans of the failing hour.
    hour_starts = [base[i].start_ns for i in in_hour]
    hour_starts += [t.start_ns for t in noise if hour_of(t.start_ns, args.start_hour) == failing_hour]
    hour_starts.sort()
    if len(set(hour_starts)) != len(hour_starts):
        raise SystemExit("gen_corpus: two failing-hour traces share a start; journey cut points would be inexact")
    journey = {}
    for k in JOURNEY_TIERS:
        got = min(k, len(hour_starts))
        cut = hour_starts[got] if got < len(hour_starts) else hour_end_ns
        journey[str(k)] = {"start_ns": hour_start_ns, "cut_ns": cut, "build": got, "requested": k, "capped": got < k}
        answers[f"journey_c.{k}"] = answer(1, value=got * lpt)
        answers[f"journey_c.{k}"]["capped"] = got < k
        answers[f"journey_c.{k}"]["requested"] = k

    truth = {
        "generator": {
            "seed": args.seed,
            "traces": args.traces,
            "spans_per_trace": spt,
            "logs_per_trace": lpt,
            "hours": args.hours,
            "scale": args.scale,
            "start_hour": args.start_hour,
            "block_rows": BLOCK_ROWS,
            "format": fmt,
            "noise_only": args.noise_only,
            "tail": args.tail,
        },
        "rows_written": {"spans": spans_sink.count, "logs": logs_sink.count},
        "unplanted_rows": {"spans": len(noise) * spt, "logs": len(noise) * lpt, "noise_traces": len(noise)},
        "failing_cell": {
            "service": SERVICES[FAILING_SERVICE],
            "hour": failing_hour,
            "hour_start_ns": hour_start_ns,
            "hour_end_ns": hour_end_ns,
            "trace_ids": failing_ids,
            "count": len(failing_ids),
            "requested": FAILING_CELL_TRACES,
            "capped": len(failing_ids) < FAILING_CELL_TRACES,
        },
        "saved_traces": saved,
        "needles": {v: len(r) for v, r in needles.items()},
        "needle_blocks": needle_blocks,
        "erasure_victim": victim,
        "request_id": {"present": present_id, "absent": REQUEST_ID_ABSENT},
        "in_lists": in_lists,
        "control_extra": control_extra,
        "journey_c": journey,
        "answers": answers,
    }
    with open(os.path.join(args.out, "truth.json"), "w") as f:
        json.dump(truth, f, indent=1, sort_keys=True)
        f.write("\n")
    print(f"wrote {spans_sink.count} spans, {logs_sink.count} logs ({fmt}) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
