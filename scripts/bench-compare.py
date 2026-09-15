#!/usr/bin/env python3
"""ADR-0070 tier B: compare a criterion bench run to a committed baseline.

Two subcommands:

  collect   walk a criterion output directory and flatten every benchmark's
            point estimate into one JSON file (the baseline format).
  compare   diff a current collect against a baseline, print a Markdown table,
            and classify each benchmark as regression / improvement / within
            threshold.

Design note (deliberate, see ADR-0070 decision 3 and issue #533): this reads
criterion's machine-readable estimates.json files, never criterion's stdout. A
grep over criterion stdout would be defeated by CARGO_TERM_COLOR=always injecting
ANSI codes into the numbers, which is a documented CI trap in this repo. Parsing
JSON sidesteps it entirely, so the compare step never depends on terminal color.

Metric: median point estimate in nanoseconds. Median is used over mean because
it is the more noise-robust central estimate on a loaded shared runner, which is
exactly the environment ADR-0070 says tier B runs in.

Exit codes for `compare`:
  0  advisory mode (default), OR enforce mode with no regression and no gap
  1  enforce mode and at least one benchmark regressed past the threshold, or a
     baseline benchmark is missing from the current run (a gap cannot be proven
     clean, so it fails closed under enforcement)
  2  a usage or input error (unreadable file, malformed JSON)

Advisory vs enforce is what makes the ADR-0070 tension in issue #533 reachable
from one tool: the default advisory behaviour matches the ADR (never fails the
build, comment only), and --enforce is the "advisory-that-can-block" the ticket
asks for, to be turned on only after the probation window the ADR requires.
"""

import argparse
import json
import os
import sys


def _die(msg, code=2):
    print(f"bench-compare: {msg}", file=sys.stderr)
    sys.exit(code)


def _load_json(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        _die(f"file not found: {path}")
    except (OSError, ValueError) as exc:
        _die(f"could not read {path}: {exc}")


def collect(args):
    root = args.criterion_dir
    if not os.path.isdir(root):
        _die(f"criterion directory not found: {root}")
    benchmarks = {}
    for dirpath, _dirnames, filenames in os.walk(root):
        # Criterion writes the latest run's estimate to <id>/new/estimates.json.
        # The <id>/base/ copy (a saved --baseline) is deliberately ignored: we
        # want the run that just happened, not a previously saved one.
        if os.path.basename(dirpath) != "new":
            continue
        if "estimates.json" not in filenames:
            continue
        bench_id = os.path.relpath(os.path.dirname(dirpath), root).replace(os.sep, "/")
        est = _load_json(os.path.join(dirpath, "estimates.json"))
        try:
            median = float(est["median"]["point_estimate"])
            mean = float(est["mean"]["point_estimate"])
        except (KeyError, TypeError, ValueError) as exc:
            _die(f"unexpected estimates.json shape in {dirpath}: {exc}")
        benchmarks[bench_id] = {"median_ns": median, "mean_ns": mean}
    if not benchmarks:
        _die(f"no estimates.json found under {root}; did the benches run?")
    meta = {
        "label": args.label,
        "note": (
            "median_ns/mean_ns are criterion point estimates in nanoseconds. "
            "See scripts/bench-compare.py and ADR-0070 decision 3."
        ),
    }
    knobs = _parse_knobs(args.knob)
    if knobs:
        meta["knobs"] = knobs
    out = {
        "_meta": meta,
        "benchmarks": dict(sorted(benchmarks.items())),
    }
    text = json.dumps(out, indent=2) + "\n"
    if args.out == "-":
        sys.stdout.write(text)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
        print(f"bench-compare: wrote {len(benchmarks)} benchmarks to {args.out}", file=sys.stderr)


def _parse_knobs(pairs):
    """Turn repeated KEY=VALUE arguments into a dict, refusing a malformed one."""
    knobs = {}
    for pair in pairs or []:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            _die(f"--knob expects KEY=VALUE, got {pair!r}")
        knobs[key] = value
    return knobs


def _knob_drift(base_doc, cur_doc):
    """Report how the two runs' sampling knobs relate.

    Returns (state, lines) where state is "match", "differ", or "unknown".
    A knob such as RAVEL_BENCH_MAX_SERIES is part of the bench id, so a
    mismatch renames an arm: it reads as missing on one side and as an ignored
    extra on the other, and leaves the comparison without failing anything.
    "unknown" is a distinct answer from "match": a file recorded before the
    knobs were stamped cannot be checked, and saying so is not the same as
    saying they agree.
    """
    base = base_doc.get("_meta", {}).get("knobs")
    cur = cur_doc.get("_meta", {}).get("knobs")
    if not base or not cur:
        side = "baseline file" if not base else "current file"
        if not base and not cur:
            side = "baseline and current files"
        return f"unknown:{side}", [
            f"- sampling knobs: NOT RECORDED on the {side}, so this "
            "comparison cannot be checked for sampling drift. An enforcing run "
            "refuses this pair: re-record the baseline through "
            "`bench-tier-b.sh record`, which stamps them."
        ]
    if base == cur:
        return "match", []
    differing = sorted(set(base) | set(cur))
    lines = [
        "- sampling knobs: MISMATCH between the baseline and this run. A knob "
        "that appears in a bench id renames its arm, so the arm drops out of "
        "the comparison instead of being compared."
    ]
    for key in differing:
        b = base.get(key, "(absent)")
        c = cur.get(key, "(absent)")
        if b != c:
            lines.append(f"  - `{key}`: baseline `{b}`, current `{c}`")
    return "differ", lines


def _pct(base, cur):
    if base == 0:
        return float("inf") if cur > 0 else 0.0
    return (cur - base) / base * 100.0


def compare(args):
    base_doc = _load_json(args.baseline)
    cur_doc = _load_json(args.current)
    base = base_doc.get("benchmarks", {})
    cur = cur_doc.get("benchmarks", {})
    if not base:
        _die(f"baseline {args.baseline} has no benchmarks")

    threshold = args.threshold
    rows = []
    regressions = []
    missing = []
    for bench_id in sorted(base):
        b = base[bench_id]["median_ns"]
        if bench_id not in cur:
            missing.append(bench_id)
            rows.append((bench_id, b, None, None, "MISSING"))
            continue
        c = cur[bench_id]["median_ns"]
        pct = _pct(b, c)
        if pct > threshold:
            status = "REGRESSION"
            regressions.append((bench_id, pct))
        elif pct < -threshold:
            status = "improvement"
        else:
            status = "ok"
        rows.append((bench_id, b, c, pct, status))

    extra = sorted(set(cur) - set(base))

    def fmt_ns(v):
        if v is None:
            return "-"
        if v >= 1e6:
            return f"{v / 1e6:.3f} ms"
        if v >= 1e3:
            return f"{v / 1e3:.3f} us"
        return f"{v:.1f} ns"

    lines = []
    base_label = base_doc.get("_meta", {}).get("label", "(unlabeled)")
    cur_label = cur_doc.get("_meta", {}).get("label", "(unlabeled)")
    mode = "ENFORCING (can block)" if args.enforce else "advisory (never blocks)"
    lines.append(f"### ADR-0070 tier B bench compare ({mode})")
    lines.append("")
    lines.append(f"- baseline: `{base_label}`")
    lines.append(f"- current: `{cur_label}`")
    lines.append(f"- threshold: +/-{threshold:g}% on median")
    knob_state, knob_lines = _knob_drift(base_doc, cur_doc)
    lines.extend(knob_lines)
    lines.append("")
    lines.append("| benchmark | baseline | current | change | status |")
    lines.append("|---|---:|---:|---:|---|")
    for bench_id, b, c, pct, status in rows:
        change = "-" if pct is None else f"{pct:+.1f}%"
        mark = {
            "REGRESSION": ":red_circle: regression",
            "improvement": ":green_circle: improvement",
            "ok": "ok",
            "MISSING": ":warning: missing from run",
        }[status]
        lines.append(f"| `{bench_id}` | {fmt_ns(b)} | {fmt_ns(c)} | {change} | {mark} |")
    lines.append("")
    if extra:
        lines.append(f"_{len(extra)} benchmark(s) in the run but not in the baseline (ignored): "
                     + ", ".join(f"`{e}`" for e in extra) + "_")
        lines.append("")
    if regressions:
        worst = max(p for _, p in regressions)
        lines.append(f"**{len(regressions)} regression(s) past +{threshold:g}%, worst {worst:+.1f}%.**")
    elif missing:
        lines.append(f"**{len(missing)} baseline benchmark(s) missing from the run.**")
    elif knob_state == "differ":
        lines.append("**The sampling knobs differ, so the two runs are not comparable.**")
    elif knob_state.startswith("unknown"):
        side = knob_state.split(":", 1)[1]
        lines.append(f"**The sampling knobs are not recorded on the {side}, so the two "
                     "runs cannot be shown to be comparable.**")
    else:
        lines.append(f"**No regression past +{threshold:g}%.**")
    lines.append("")
    if not args.enforce:
        lines.append("_Advisory per ADR-0070 decision 3: this comment never fails the build. "
                     "Promotion to enforcing is gated on the probation window in ADR-0070's "
                     "amendment (issue #533)._")

    report = "\n".join(lines) + "\n"
    if args.out_md and args.out_md != "-":
        with open(args.out_md, "w", encoding="utf-8") as fh:
            fh.write(report)
    sys.stdout.write(report)

    # "unknown" fails an enforcing run as well as "differ". A baseline whose
    # sampling knobs were never recorded cannot be shown to have been measured
    # the same way, and enforcing against it would report agreement it never
    # checked. Advisory runs say so and still exit 0, per ADR-0070 decision 3.
    fail = bool(regressions) or bool(missing) or knob_state != "match"
    if args.enforce and fail:
        sys.exit(1)
    sys.exit(0)


def main():
    ap = argparse.ArgumentParser(description="ADR-0070 tier B bench compare")
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("collect", help="flatten a criterion dir into a baseline JSON")
    c.add_argument("--criterion-dir", required=True)
    c.add_argument("--out", required=True, help="output path, or - for stdout")
    c.add_argument("--label", required=True, help="provenance label baked into the file")
    c.add_argument(
        "--knob",
        action="append",
        metavar="KEY=VALUE",
        help="sampling knob stamped into _meta.knobs; repeatable",
    )
    c.set_defaults(func=collect)

    d = sub.add_parser("compare", help="diff a current collect against a baseline")
    d.add_argument("--baseline", required=True)
    d.add_argument("--current", required=True)
    d.add_argument("--threshold", type=float, default=15.0, help="percent, default 15")
    d.add_argument("--enforce", action="store_true", help="exit non-zero on regression")
    d.add_argument("--out-md", help="also write the Markdown table to this path")
    d.set_defaults(func=compare)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
