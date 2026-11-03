#!/usr/bin/env python3
"""Deterministic OTLP fixture for the MCP agent evaluation (issue #1385).

Every value is a fixed function of the anchor T0 (a minute-aligned unix
nanosecond timestamp the loader picks and records) and of SEED. Nothing reads
the clock, the environment or a random source the seed does not drive.

    gen.py counts                      print the fixture's counts as JSON
    gen.py encode SIGNAL ANCHOR_NS     write the OTLP request body for SIGNAL
                                       (metrics, logs or traces) to stdout
    gen.py late BATCH_NS LABEL         write the 4-row late log batch carrying
                                       eval.batch=LABEL, at BATCH_NS + 0..3 s
                                       (a minute-aligned time the runner picks
                                       inside the ingest-lag window), to stdout

Standard library only: the protobuf wire encoding is written out by hand
against the opentelemetry-proto field numbers.
"""

import json
import struct
import sys

SEED = 1385
SERVICES = ["checkout", "payments", "inventory", "frontend"]
MINUTE_NS = 60 * 1_000_000_000
SECOND_NS = 1_000_000_000
MS_NS = 1_000_000

SCRAPE_STEP_NS = 15 * SECOND_NS
SCRAPES = 240  # 60 minutes at 15 s
TRACE_STEP_NS = 30 * SECOND_NS
TRACES = 120  # 60 minutes at 30 s
SLOW_FIRST, SLOW_LAST = 40, 59  # trace index range in [T0+20m, T0+30m)
SLOW_SCRAPE_FIRST, SLOW_SCRAPE_LAST = 80, 119  # scrape index range, same window
FAILING = (44, 49, 54)
TARGET_USER = "u-7f3a"
TARGET_TRACES = (12, 47, 95)
LATE_ROWS = 4

SEV_INFO, SEV_WARN, SEV_ERROR = 9, 13, 17
KIND_SERVER = 2
STATUS_ERROR = 2


class Lcg:
    """64-bit linear congruential generator (Knuth MMIX constants)."""

    def __init__(self, seed):
        self.state = seed & 0xFFFFFFFFFFFFFFFF

    def below(self, n):
        self.state = (self.state * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        return (self.state >> 33) % n


def trace_id(j):
    return bytes.fromhex("5eed%04x%08x%016x" % (SEED, 0, j + 1))


def span_id(j, k):
    return bytes.fromhex("%012x%04x" % (j + 1, k + 1))


# --- model -----------------------------------------------------------------


def metric_points(t0):
    """(family, otlp_name, unit, kind, service, attrs, [(ts, value)])."""
    out = []
    for s, svc in enumerate(SERVICES):
        for status_class in ("2xx", "5xx"):
            pts = []
            total = 0
            for k in range(SCRAPES):
                if status_class == "2xx":
                    total += 10
                elif svc == "checkout" and SLOW_SCRAPE_FIRST <= k <= SLOW_SCRAPE_LAST:
                    total += 3
                pts.append((t0 + k * SCRAPE_STEP_NS, float(total)))
            out.append(("http_server_requests_total", "http_server_requests_total", "",
                        "sum", svc, {"status_class": status_class}, pts))
        pts = []
        for k in range(SCRAPES):
            slow = svc == "checkout" and SLOW_SCRAPE_FIRST <= k <= SLOW_SCRAPE_LAST
            pts.append((t0 + k * SCRAPE_STEP_NS, float((900 if slow else 40) + k % 5)))
        out.append(("http_server_duration_p99_milliseconds", "http_server_duration_p99", "ms",
                    "gauge", svc, {}, pts))
        pts = [(t0 + k * SCRAPE_STEP_NS, float(200_000_000 + 1_000_000 * s + 1000 * k))
               for k in range(SCRAPES)]
        out.append(("process_resident_memory_bytes", "process_resident_memory", "By",
                    "gauge", svc, {}, pts))
    return out


# One trace: frontend root -> checkout -> (payments, inventory).
SPAN_SHAPE = [
    # (service, name, parent index, base duration ms, start offset ms)
    ("frontend", "GET /checkout", None, 120, 0),
    ("checkout", "POST /orders", 0, 80, 5),
    ("payments", "charge", 1, 30, 10),
    ("inventory", "reserve", 1, 20, 45),
]


def spans(t0):
    """Span dicts in generation order."""
    rng = Lcg(SEED)
    out = []
    for j in range(TRACES):
        start = t0 + j * TRACE_STEP_NS
        slow = SLOW_FIRST <= j <= SLOW_LAST
        failing = j in FAILING
        user = TARGET_USER if j in TARGET_TRACES else "u-%04d" % (j % 10 + 1)
        for k, (svc, name, parent, dur_ms, off_ms) in enumerate(SPAN_SHAPE):
            dur = dur_ms + rng.below(5)
            if slow and svc == "checkout":
                dur = 900 + rng.below(5)
            if slow and svc == "frontend":
                dur = 950 + rng.below(5)
            attrs = {"session.id": "s-%04d" % j}
            if k == 0:
                attrs["user.id"] = user
                attrs["http.status_code"] = 500 if failing else 200
            status = None
            if failing and svc == "payments":
                status = "card processor timeout"
            if failing and svc == "checkout":
                status = "upstream payments failed"
            s_start = start + off_ms * MS_NS
            out.append({
                "service": svc, "name": name, "trace": j, "k": k,
                "trace_id": trace_id(j), "span_id": span_id(j, k),
                "parent": None if parent is None else span_id(j, parent),
                "start": s_start, "end": s_start + dur * MS_NS,
                "attrs": attrs, "error": status,
            })
    return out


def logs(t0):
    """Log dicts: one per span, one heartbeat a minute per service, 5 auth events."""
    out = []
    for sp in spans(t0):
        attrs = {"session.id": sp["attrs"]["session.id"]}
        if "user.id" in sp["attrs"]:
            attrs["user.id"] = sp["attrs"]["user.id"]
        if sp["error"]:
            body, sev, text = "%s failed: %s" % (sp["name"], sp["error"]), SEV_ERROR, "ERROR"
        else:
            body, sev, text = "%s handled %s" % (sp["service"], sp["name"]), SEV_INFO, "INFO"
        out.append({"service": sp["service"], "ts": sp["start"] + MS_NS, "sev": sev,
                    "text": text, "body": body, "trace_id": sp["trace_id"],
                    "span_id": sp["span_id"], "attrs": attrs})
    for m in range(60):
        for svc in SERVICES:
            out.append({"service": svc, "ts": t0 + m * MINUTE_NS + 500 * MS_NS, "sev": SEV_INFO,
                        "text": "INFO", "body": "heartbeat", "trace_id": None, "span_id": None,
                        "attrs": {}})
    auth = [
        (33 * MINUTE_NS, SEV_WARN, "WARN", "login failed: bad password"),
        (33 * MINUTE_NS + 10 * SECOND_NS, SEV_WARN, "WARN", "login failed: bad password"),
        (33 * MINUTE_NS + 20 * SECOND_NS, SEV_WARN, "WARN", "login failed: bad password"),
        (34 * MINUTE_NS, SEV_INFO, "INFO", "password reset requested"),
        (35 * MINUTE_NS, SEV_INFO, "INFO", "login succeeded"),
    ]
    for off, sev, text, body in auth:
        out.append({"service": "frontend", "ts": t0 + off, "sev": sev, "text": text,
                    "body": body, "trace_id": None, "span_id": None,
                    "attrs": {"user.id": TARGET_USER, "session.id": "s-auth",
                              "event.category": "authentication"}})
    return out


def late_logs(batch_ns, label):
    return [{"service": "checkout", "ts": batch_ns + i * SECOND_NS, "sev": SEV_INFO,
             "text": "INFO", "body": "late batch row %d" % i, "trace_id": None,
             "span_id": None, "attrs": {"eval.batch": label}} for i in range(LATE_ROWS)]


def counts():
    t0 = 0
    mp = metric_points(t0)
    sp = spans(t0)
    lg = logs(t0)
    return {
        "series": len(mp),
        "samples": sum(len(m[6]) for m in mp),
        "metric_families": len({m[0] for m in mp}),
        "log_rows": len(lg),
        "spans": len(sp),
        "traces": len({s["trace"] for s in sp}),
        "slow_traces": SLOW_LAST - SLOW_FIRST + 1,
        "failing_traces": len({s["trace"] for s in sp if s["error"]}),
        "error_spans": sum(1 for s in sp if s["error"]),
        "error_log_rows": sum(1 for row in lg if row["sev"] == SEV_ERROR),
        "target_user_log_rows": sum(1 for row in lg if row["attrs"].get("user.id") == TARGET_USER),
        "target_user_spans": sum(1 for s in sp if s["attrs"].get("user.id") == TARGET_USER),
        "late_batch_rows": len(late_logs(t0, "x")),
    }


# --- protobuf wire encoding ------------------------------------------------


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def tag(field, wire):
    return varint(field << 3 | wire)


def f_len(field, payload):
    return tag(field, 2) + varint(len(payload)) + payload


def f_str(field, s):
    return f_len(field, s.encode("utf-8"))


def f_varint(field, n):
    return tag(field, 0) + varint(n)


def f_fixed64(field, n):
    return tag(field, 1) + struct.pack("<Q", n)


def f_double(field, x):
    return tag(field, 1) + struct.pack("<d", x)


def any_value(v):
    if isinstance(v, int):
        return f_varint(3, v & 0xFFFFFFFFFFFFFFFF)
    return f_str(1, v)


def kv(key, value):
    return f_str(1, key) + f_len(2, any_value(value))


def resource(svc):
    attrs = f_len(1, kv("service.name", svc)) + f_len(1, kv("service.instance.id", svc + "-0"))
    return f_len(1, attrs)


def scope():
    return f_len(1, f_str(1, "ravel-mcp-eval") + f_str(2, "1385"))


def encode_metrics(t0):
    by_service = {}
    for family, name, unit, kind, svc, attrs, pts in metric_points(t0):
        dps = b""
        for ts, value in pts:
            dp = b"".join(f_len(7, kv(k, v)) for k, v in sorted(attrs.items()))
            if kind == "sum":
                dp += f_fixed64(2, t0)
            dp += f_fixed64(3, ts) + f_double(4, value)
            dps += f_len(1, dp)
        metric = f_str(1, name) + (f_str(3, unit) if unit else b"")
        if kind == "sum":
            metric += f_len(7, dps + f_varint(2, 2) + f_varint(3, 1))  # cumulative, monotonic
        else:
            metric += f_len(5, dps)
        by_service.setdefault(svc, []).append(f_len(2, metric))
    body = b""
    for svc in SERVICES:
        sm = scope() + b"".join(by_service[svc])
        body += f_len(1, resource(svc) + f_len(2, sm))
    return body


def encode_logs_rows(rows):
    by_service = {}
    for row in rows:
        rec = f_fixed64(1, row["ts"]) + f_varint(2, row["sev"]) + f_str(3, row["text"])
        rec += f_len(5, any_value(row["body"]))
        rec += b"".join(f_len(6, kv(k, v)) for k, v in sorted(row["attrs"].items()))
        if row["trace_id"] is not None:
            rec += f_len(9, row["trace_id"]) + f_len(10, row["span_id"])
        rec += f_fixed64(11, row["ts"])
        by_service.setdefault(row["service"], []).append(f_len(2, rec))
    body = b""
    for svc in SERVICES:
        if svc in by_service:
            sl = scope() + b"".join(by_service[svc])
            body += f_len(1, resource(svc) + f_len(2, sl))
    return body


def encode_traces(t0):
    by_service = {}
    for sp in spans(t0):
        msg = f_len(1, sp["trace_id"]) + f_len(2, sp["span_id"])
        if sp["parent"] is not None:
            msg += f_len(4, sp["parent"])
        msg += f_str(5, sp["name"]) + f_varint(6, KIND_SERVER)
        msg += f_fixed64(7, sp["start"]) + f_fixed64(8, sp["end"])
        msg += b"".join(f_len(9, kv(k, v)) for k, v in sorted(sp["attrs"].items()))
        if sp["error"]:
            msg += f_len(15, f_str(2, sp["error"]) + f_varint(3, STATUS_ERROR))
        by_service.setdefault(sp["service"], []).append(f_len(2, msg))
    body = b""
    for svc in SERVICES:
        ss = scope() + b"".join(by_service[svc])
        body += f_len(1, resource(svc) + f_len(2, ss))
    return body


def parse_anchor(text):
    anchor = int(text)
    if anchor <= 0 or anchor % MINUTE_NS:
        raise SystemExit("gen.py: anchor must be a positive minute-aligned unix ns value")
    return anchor


def main(argv):
    if len(argv) == 2 and argv[1] == "counts":
        print(json.dumps(counts(), sort_keys=True))
        return 0
    if len(argv) == 4 and argv[1] == "encode":
        t0 = parse_anchor(argv[3])
        encoders = {"metrics": encode_metrics, "logs": lambda a: encode_logs_rows(logs(a)),
                    "traces": encode_traces}
        if argv[2] not in encoders:
            raise SystemExit("gen.py: unknown signal %r" % argv[2])
        sys.stdout.buffer.write(encoders[argv[2]](t0))
        return 0
    if len(argv) == 4 and argv[1] == "late":
        sys.stdout.buffer.write(encode_logs_rows(late_logs(parse_anchor(argv[2]), argv[3])))
        return 0
    sys.stderr.write(__doc__)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv))
