#!/usr/bin/env python3
"""Request bodies for the released-binary fence drill, with no dependency
beyond the standard library: OTLP/HTTP protobuf for metrics, logs and
traces, and a snappy-framed Prometheus remote-write v1 request.

usage: drill_payload.py otlp-metrics|otlp-logs|otlp-traces|remote-write SEQ > body

SEQ makes every body distinct (series value, log body, span id), so two
requests never collapse into one idempotent write. Timestamps are now.
"""

import struct
import sys
import time


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


def key(num, wire):
    return varint((num << 3) | wire)


def f_bytes(num, data):
    return key(num, 2) + varint(len(data)) + data


def f_str(num, s):
    return f_bytes(num, s.encode())


def f_varint(num, n):
    return key(num, 0) + varint(n)


def f_fixed64(num, n):
    return key(num, 1) + struct.pack("<Q", n)


def f_double(num, x):
    return key(num, 1) + struct.pack("<d", x)


def kv(k, v):
    return f_str(1, k) + f_bytes(2, f_str(1, v))


def resource(service):
    return f_bytes(1, f_bytes(1, kv("service.name", service)))


SCOPE = f_bytes(1, f_str(1, "ravel-compat-drill"))


def otlp_metrics(seq, now_ns):
    point = (
        f_bytes(7, kv("drill", "fence"))
        + f_fixed64(2, now_ns - 1_000_000_000)
        + f_fixed64(3, now_ns)
        + f_double(4, float(seq))
    )
    metric = f_str(1, "drill_otlp_gauge") + f_bytes(5, f_bytes(1, point))
    scope_metrics = SCOPE + f_bytes(2, metric)
    return f_bytes(1, resource("drill-otlp") + f_bytes(2, scope_metrics))


def otlp_logs(seq, now_ns):
    record = (
        f_fixed64(1, now_ns)
        + f_fixed64(11, now_ns)
        + f_varint(2, 9)
        + f_str(3, "INFO")
        + f_bytes(5, f_str(1, "drill log line %d" % seq))
        + f_bytes(6, kv("drill", "fence"))
    )
    scope_logs = SCOPE + f_bytes(2, record)
    return f_bytes(1, resource("drill-logs") + f_bytes(2, scope_logs))


def otlp_traces(seq, now_ns):
    trace_id = struct.pack(">QQ", 0xD1F1, seq + 1)
    span_id = struct.pack(">Q", seq + 1)
    span = (
        f_bytes(1, trace_id)
        + f_bytes(2, span_id)
        + f_str(5, "drill-span")
        + f_varint(6, 2)
        + f_fixed64(7, now_ns - 5_000_000)
        + f_fixed64(8, now_ns)
        + f_bytes(9, kv("drill", "fence"))
    )
    scope_spans = SCOPE + f_bytes(2, span)
    return f_bytes(1, resource("drill-traces") + f_bytes(2, scope_spans))


def snappy_literal(data):
    """A valid snappy block made only of literal elements."""
    out = bytearray(varint(len(data)))
    for i in range(0, len(data), 65536):
        chunk = data[i : i + 65536]
        n = len(chunk) - 1
        if n < 60:
            out.append(n << 2)
        elif n < 0x100:
            out += bytes([60 << 2, n])
        else:
            out += bytes([61 << 2]) + struct.pack("<H", n)
        out += chunk
    return bytes(out)


def remote_write(seq, now_ns):
    labels = [("__name__", "drill_rw_gauge"), ("drill", "fence"), ("job", "drill")]
    ts = b"".join(f_bytes(1, f_str(1, k) + f_str(2, v)) for k, v in labels)
    ts += f_bytes(2, f_double(1, float(seq)) + f_varint(2, now_ns // 1_000_000))
    return snappy_literal(f_bytes(1, ts))


KINDS = {
    "otlp-metrics": otlp_metrics,
    "otlp-logs": otlp_logs,
    "otlp-traces": otlp_traces,
    "remote-write": remote_write,
}


def main(argv):
    if len(argv) != 3 or argv[1] not in KINDS:
        sys.stderr.write(__doc__)
        return 64
    body = KINDS[argv[1]](int(argv[2]), time.time_ns())
    sys.stdout.buffer.write(body)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
