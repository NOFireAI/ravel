#!/usr/bin/env python3
"""Build or check bench/baselines/s3-envelope.json from bench-s3 lane reports.

Usage:
  scripts/bench/s3-envelope.py summarise --out FILE --runs-on LABEL \\
      --report RUN_ID:DATE:report-s3.json [--report ...] \\
      [--unexplained TEXT ...]
  scripts/bench/s3-envelope.py check FILE

summarise reads each run's report-s3.json (the artifact the bench-s3 workflow
uploads), refuses reports whose environments differ, and writes the envelope:
the environment, one summary entry per run, the min and max of every figure
scripts/bench-s3-compare.py compares, and the provenance in `_meta`.
`--unexplained` carries an open question about the data into `_meta`; restate
every one still open, since nothing is copied from an earlier envelope.

check recomputes the min and max from the file's own `runs` and exits 1 when
the stored `envelope` member disagrees, so the file cannot be edited by hand
out of agreement with the runs it summarises.

Exit codes: 0 written or in agreement, 1 check found a disagreement, 2 bad
input (unreadable report, environments that differ, a malformed argument).
"""

import argparse
import importlib.util
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "bench_s3_compare", os.path.join(_HERE, "..", "bench-s3-compare.py")
)
bench_s3_compare = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench_s3_compare)

WORKFLOW = "bench-s3"
LOAD_POINT = (
    "bench_report --store s3 --shards 4 --target-series 1000 "
    "--points-per-sec 50000 --duration-secs 10"
)
SCOPE = (
    "This is the envelope at one load point on a shared hosted runner and is "
    "comparable only to runs of the same lane."
)


class InputError(Exception):
    pass


def _dig(doc, *path):
    value = bench_s3_compare._dig(doc, path)
    if value is None:
        raise InputError(f"report has no {'.'.join(path)}")
    return value


def environment_of(report):
    env = report.get("environment")
    if not isinstance(env, dict):
        raise InputError("report has no environment member")
    out = {f: env.get(f) for f in bench_s3_compare.ENV_FIELDS}
    out["workload"] = env.get("workload")
    return out


def run_entry(run_id, date, report):
    def lat(section, name):
        block = _dig(report, section, name)
        return {q: _dig(block, q) for q in ("p50", "p95", "p99")}

    return {
        "run_id": run_id,
        "scheduled": date,
        "git_commit": _dig(report, "environment", "git_commit"),
        "strict_ack_latency_ms": lat("ingest", "strict_ack_latency_ms"),
        "cold_latency_ms_p50": _dig(report, "query", "cold_latency_ms", "p50"),
        "warm_latency_ms": lat("query", "warm_latency_ms"),
        "accepted_points": _dig(report, "ingest", "accepted_points"),
        "matched_series": _dig(report, "query", "matched_series"),
        "s3_requests": {k: _dig(report, "s3_requests", k) for k in ("put", "get", "list")},
        "bytes": {k: _dig(report, "bytes", k) for k in ("written", "read")},
    }


def build_document(environment, runs, runs_on, unexplained):
    """The envelope file's content. The `envelope` member is always computed."""
    env = environment
    return {
        "_meta": {
            "workflow": WORKFLOW,
            "runs": [{"run_id": r["run_id"], "scheduled": r["scheduled"]} for r in runs],
            "runs_on": runs_on,
            "region": env["region"],
            "max_flush_delay_ms": env["max_flush_delay_ms"],
            "load_point": LOAD_POINT,
            "scope": SCOPE,
            "bands": (
                "scripts/bench-s3-compare.py: latency figures warn past the max "
                f"+{bench_s3_compare.LATENCY_BAND_PCT:g}%, GET, LIST and bytes past "
                f"the max +{bench_s3_compare.COUNT_BAND_PCT:g}%, PUT, accepted points "
                "and matched series must equal the envelope exactly."
            ),
            "unexplained": list(unexplained),
        },
        "environment": env,
        "runs": runs,
        "envelope": bench_s3_compare.compute_envelope(runs),
    }


def _parse_report_arg(text):
    parts = text.split(":", 2)
    if len(parts) != 3 or not all(parts):
        raise InputError(f"--report expects RUN_ID:DATE:PATH, got {text!r}")
    run_id, date, path = parts
    try:
        run_id = int(run_id)
    except ValueError as exc:
        raise InputError(f"--report run id is not an integer: {run_id!r}") from exc
    return run_id, date, path


def _load(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError) as exc:
        raise InputError(f"could not read {path}: {exc}") from exc


def summarise(args):
    runs = []
    environment = None
    for text in args.report:
        run_id, date, path = _parse_report_arg(text)
        report = _load(path)
        env = environment_of(report)
        if environment is None:
            environment = env
        elif env != environment:
            raise InputError(f"run {run_id} ran in a different environment: {env!r}")
        runs.append(run_entry(run_id, date, report))
    runs.sort(key=lambda r: r["scheduled"])
    doc = build_document(environment, runs, args.runs_on, args.unexplained or [])
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(doc, indent=2) + "\n")
    print(f"s3-envelope: wrote {len(runs)} runs to {args.out}", file=sys.stderr)
    return 0


def check(args):
    doc = _load(args.file)
    try:
        expected = bench_s3_compare.compute_envelope(doc.get("runs") or [])
    except bench_s3_compare.NotAComparison as exc:
        raise InputError(str(exc)) from exc
    stored = doc.get("envelope")
    if stored != expected:
        for name in sorted(set(expected) | set(stored or {})):
            if (stored or {}).get(name) != expected.get(name):
                print(f"s3-envelope: {name}: stored {(stored or {}).get(name)!r}, "
                      f"recomputed {expected.get(name)!r}", file=sys.stderr)
        return 1
    print(f"s3-envelope: {args.file} agrees with its {len(doc['runs'])} runs", file=sys.stderr)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="build or check the real-S3 envelope")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("summarise", help="build the envelope from report-s3.json files")
    s.add_argument("--out", required=True)
    s.add_argument("--runs-on", required=True, help="the lane's runs-on label, verbatim")
    s.add_argument("--report", action="append", required=True, metavar="RUN_ID:DATE:PATH")
    s.add_argument("--unexplained", action="append", metavar="TEXT")
    s.set_defaults(func=summarise)
    c = sub.add_parser("check", help="recompute the envelope member and compare")
    c.add_argument("file")
    c.set_defaults(func=check)
    args = ap.parse_args(argv)
    try:
        return args.func(args)
    except InputError as exc:
        print(f"s3-envelope: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
