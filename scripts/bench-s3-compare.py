#!/usr/bin/env python3
"""Compare one real-S3 bench_report run to the committed envelope.

Usage:
  scripts/bench-s3-compare.py --envelope bench/baselines/s3-envelope.json \\
      --report bench/reports/report-s3.json [--enforce]

Band reasoning: the five runs the envelope summarises were taken at the same
load point on a shared hosted runner, and their latency figures disagree. As
how far the envelope max sits above its min: strict-ack p50 by 40 percent,
strict-ack p99 by 29 percent, warm p50 by 39 percent and cold p50 by 53
percent. A latency figure therefore warns only past the envelope max plus 25
percent. Request and byte counts are not timings, but they move between runs
too: GET spans 47,205 to 78,279, by 66 percent, stepping up between the
2026-09-21 and 2026-09-28 runs (one of the envelope's unexplained items), and
bytes read by 10 percent. They warn past the max plus 10 percent, and so do
the billed attempt counts (put_attempts, get_attempts, list_attempts), which
S3 charges on and which a retry regression moves while the call counts stay
put. PUTs,
accepted points and matched series, identical across every run, must equal the
envelope exactly.

This is a separate script from bench-compare.py, not a mode of it: that tool
compares criterion point estimates with one symmetric threshold, while this one
reads a bench_report JSON, checks the environment before comparing anything,
and applies a per-figure band against a range rather than a single number.

Exit codes:
  0  advisory mode (default), or --enforce with every figure inside its band
  1  --enforce and at least one figure outside its band or missing
  2  not a comparison: the environments differ, or an input is unreadable
"""

import argparse
import json
import sys

LATENCY_BAND_PCT = 25.0
COUNT_BAND_PCT = 10.0

# (figure name, path in a bench_report JSON, path in an envelope `runs` entry,
# band kind). The figure name is the report path, so a message naming the
# figure also names where it was looked for.
FIGURES = (
    ("ingest.strict_ack_latency_ms.p50", ("ingest", "strict_ack_latency_ms", "p50"),
     ("strict_ack_latency_ms", "p50"), "latency"),
    ("ingest.strict_ack_latency_ms.p95", ("ingest", "strict_ack_latency_ms", "p95"),
     ("strict_ack_latency_ms", "p95"), "latency"),
    ("ingest.strict_ack_latency_ms.p99", ("ingest", "strict_ack_latency_ms", "p99"),
     ("strict_ack_latency_ms", "p99"), "latency"),
    ("query.warm_latency_ms.p50", ("query", "warm_latency_ms", "p50"),
     ("warm_latency_ms", "p50"), "latency"),
    ("query.warm_latency_ms.p95", ("query", "warm_latency_ms", "p95"),
     ("warm_latency_ms", "p95"), "latency"),
    ("query.warm_latency_ms.p99", ("query", "warm_latency_ms", "p99"),
     ("warm_latency_ms", "p99"), "latency"),
    ("query.cold_latency_ms.p50", ("query", "cold_latency_ms", "p50"),
     ("cold_latency_ms_p50",), "latency"),
    ("s3_requests.put", ("s3_requests", "put"), ("s3_requests", "put"), "exact"),
    ("s3_requests.get", ("s3_requests", "get"), ("s3_requests", "get"), "count"),
    ("s3_requests.list", ("s3_requests", "list"), ("s3_requests", "list"), "count"),
    ("s3_requests.put_attempts", ("s3_requests", "put_attempts"),
     ("s3_requests", "put_attempts"), "count"),
    ("s3_requests.get_attempts", ("s3_requests", "get_attempts"),
     ("s3_requests", "get_attempts"), "count"),
    ("s3_requests.list_attempts", ("s3_requests", "list_attempts"),
     ("s3_requests", "list_attempts"), "count"),
    ("bytes.read", ("bytes", "read"), ("bytes", "read"), "count"),
    ("bytes.written", ("bytes", "written"), ("bytes", "written"), "count"),
    ("ingest.accepted_points", ("ingest", "accepted_points"), ("accepted_points",), "exact"),
    ("query.matched_series", ("query", "matched_series"), ("matched_series",), "exact"),
)

ENV_FIELDS = ("store_backend", "region", "shard_count", "max_flush_delay_ms")

_BAND_PCT = {"latency": LATENCY_BAND_PCT, "count": COUNT_BAND_PCT}


class NotAComparison(Exception):
    pass


def _dig(doc, path):
    cur = doc
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def compute_envelope(runs):
    """Min and max of every compared figure over the envelope's runs."""
    if not runs:
        raise NotAComparison("the envelope has no runs")
    env = {}
    for name, _rpath, run_path, _kind in FIGURES:
        values = []
        for i, run in enumerate(runs):
            v = _number(_dig(run, run_path))
            if v is None:
                raise NotAComparison(f"run {i} has no numeric {'.'.join(run_path)}")
            values.append(v)
        env[name] = {"min": min(values), "max": max(values)}
    return env


def check_environment(envelope_env, report_env):
    """Raise NotAComparison naming the first environment field that differs."""
    if not isinstance(report_env, dict):
        raise NotAComparison("the report has no environment member")
    for field in ENV_FIELDS:
        if envelope_env.get(field) != report_env.get(field):
            raise NotAComparison(
                f"environment field `{field}` differs: envelope "
                f"{envelope_env.get(field)!r}, report {report_env.get(field)!r}"
            )
    ew = envelope_env.get("workload") or {}
    rw = report_env.get("workload") or {}
    for field in sorted(set(ew) | set(rw)):
        if ew.get(field) != rw.get(field):
            raise NotAComparison(
                f"environment field `workload.{field}` differs: envelope "
                f"{ew.get(field)!r}, report {rw.get(field)!r}"
            )


def band(kind, lo, hi):
    """Return (band text, check) where check(value) is True when value is outside."""
    if kind == "exact":
        if lo != hi:
            raise NotAComparison(f"an exact figure has a range in the envelope ({lo} to {hi})")
        return f"== {lo}", lambda v: v != lo
    pct = _BAND_PCT[kind]
    limit = hi * (1.0 + pct / 100.0)
    return f"<= max +{pct:g}% ({limit:.6g})", lambda v: v > limit


def compare(envelope_doc, report):
    """Return (lines, warnings). Raises NotAComparison on an unusable pair."""
    check_environment(envelope_doc.get("environment") or {}, report.get("environment"))
    stored = envelope_doc.get("envelope") or {}
    lines = []
    warnings = []
    for name, rpath, _run_path, kind in FIGURES:
        bounds = stored.get(name)
        if not isinstance(bounds, dict) or "min" not in bounds or "max" not in bounds:
            raise NotAComparison(f"the envelope has no bounds for {name}")
        lo, hi = bounds["min"], bounds["max"]
        text, is_outside = band(kind, lo, hi)
        value = _number(_dig(report, rpath))
        if value is None:
            lines.append(f"{name}: MISSING band {text} envelope min {lo} max {hi}")
            warnings.append(f"{name} is missing from the report")
            continue
        outside = is_outside(value)
        state = "OUTSIDE" if outside else "ok"
        lines.append(f"{name}: {value} band {text} envelope min {lo} max {hi} {state}")
        if outside:
            warnings.append(f"{name} = {value} is outside its band {text} (envelope min {lo}, max {hi})")
    return lines, warnings


def _load(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as exc:
        raise NotAComparison(f"could not read {path}: {exc}") from exc


def main(argv=None):
    ap = argparse.ArgumentParser(description="compare a real-S3 bench report to the committed envelope")
    ap.add_argument("--envelope", required=True)
    ap.add_argument("--report", required=True)
    ap.add_argument("--enforce", action="store_true", help="exit 1 when a figure is outside its band")
    args = ap.parse_args(argv)
    try:
        envelope_doc = _load(args.envelope)
        report = _load(args.report)
        lines, warnings = compare(envelope_doc, report)
    except NotAComparison as exc:
        print(f"bench-s3-compare: not a comparison: {exc}", file=sys.stderr)
        # On stdout too, as an annotation, so a refused pair is visible in the run.
        print(f"::warning::bench-s3-compare: not a comparison: {exc}")
        return 2
    mode = "enforcing" if args.enforce else "advisory"
    print(f"bench-s3-compare ({mode}): {args.report} against {args.envelope}")
    for line in lines:
        print(line)
    for w in warnings:
        print(f"::warning::{w}")
    print(f"{len(warnings)} of {len(FIGURES)} figures outside their band or missing")
    if args.enforce and warnings:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
