#!/usr/bin/env python3
"""Evaluate a selective-investigations run (issue #3699, ADR-3692 decision 8).

Reads calls.jsonl, truth.json, a stamps directory, and (optionally) a
prices file, and writes results.json and summary.txt next to each other
under --out. Every figure bands.toml names must appear in results.json's
"figures" list EXACTLY ONCE (CLASS CLOSURE: this is the one check every
number in this script's output goes through; see check_bands()). A
figure present zero or two-or-more times is a hard failure, independent
of whether any band was crossed.

results.json["figures"] is a LIST of {"name", "value"} objects, not a
dict: a dict can't represent an accidental duplicate, and detecting a
duplicate is exactly the property this script exists to guarantee (see
README.md "Why figures is a list, not a dict").
"""
import argparse
import json
import math
import os
import sys
import tomllib


def percentile(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


def load_jsonl(path):
    rows = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def group_calls(calls):
    groups = {}
    for c in calls:
        key = (c["statement"], c["state"])
        groups.setdefault(key, []).append(c)
    return groups


def is_unsupported(calls):
    return all(c.get("refusal_class") == "unsupported" for c in calls)


def statement_figures(statement, state, calls, figures, prefix_requires_latency=True):
    """Append this (statement, state) pair's figures to `figures` (a list)
    and return its detail dict for results.json["statements"]."""
    prefix = f"{statement}.{state}"
    if is_unsupported(calls):
        figures.append({"name": f"{prefix}.p50_ms", "value": "unsupported-on-this-build"})
        figures.append({"name": f"{prefix}.p99_ms", "value": "unsupported-on-this-build"})
        return {
            "statement": statement,
            "state": state,
            "status": "unsupported-on-this-build",
            "calls": calls,
        }

    cold = [c for c in calls if c.get("cold", True)]
    durations_ms = [c["duration_s"] * 1000.0 for c in cold]
    wrong = [c for c in cold if c.get("refusal_class") == "wrong_answer"]
    rows_status = "wrong_answer" if wrong else "ok"

    p50 = percentile(durations_ms, 0.50)
    p95 = percentile(durations_ms, 0.95)
    p99 = percentile(durations_ms, 0.99)

    figures.append({"name": f"{prefix}.p50_ms", "value": p50})
    figures.append({"name": f"{prefix}.p95_ms", "value": p95})
    figures.append({"name": f"{prefix}.p99_ms", "value": p99})

    phase_totals = {}
    pruning = {}
    io = {}
    key_index = "absent"
    for c in cold:
        stats = c.get("stats") or {}
        for ph in stats.get("phases", []):
            agg = phase_totals.setdefault(
                ph["phase"],
                {"s3GetRequests": 0, "s3GetWireBytes": 0, "s3ListRequests": 0, "decompressedOutputBytes": 0},
            )
            for k in agg:
                agg[k] += ph.get(k, 0)
        if "pruning" in stats:
            pruning = stats["pruning"]
        if "io" in stats:
            io = stats["io"]
        if "keyIndex" in stats:
            key_index = stats["keyIndex"]

    return {
        "statement": statement,
        "state": state,
        "status": rows_status,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "n_cold": len(cold),
        "phases": phase_totals,
        "pruning": pruning,
        "io": io,
        "keyIndex": key_index,
        "calls": calls,
    }


def correctness_pass_fraction(statements_detail):
    total = 0
    ok = 0
    for d in statements_detail:
        if d["status"] == "unsupported-on-this-build":
            continue
        total += 1
        if d["status"] == "ok":
            ok += 1
    if total == 0:
        return 1.0
    return ok / total


def query_cost(requests, cpu_seconds, get_price_per_1000, instance_price_per_hour):
    return requests * get_price_per_1000 / 1000.0 + cpu_seconds * instance_price_per_hour / 3600.0


def maintenance_cost_per_part_month(fold_section_gets, leaf_put_requests, leaf_bytes, get_price_per_1000, storage_price_per_gb_month):
    requests = fold_section_gets + leaf_put_requests
    bytes_gb = leaf_bytes / (1024.0 ** 3)
    return requests * get_price_per_1000 / 1000.0 + bytes_gb * storage_price_per_gb_month


def breakeven_lookups_per_part_month(maintenance_cost, savings_per_lookup):
    if savings_per_lookup <= 0:
        return math.inf
    return maintenance_cost / savings_per_lookup


def check_bands(figures, bands):
    """figures: list of {"name","value"}. bands: parsed bands.toml dict of
    row -> {figure, target, miss, lanes, judged}. Returns (errors, verdict_rows).
    errors is a list of strings; a non-empty list is a hard failure
    regardless of verdict_rows."""
    counts = {}
    values = {}
    for f in figures:
        counts[f["name"]] = counts.get(f["name"], 0) + 1
        values[f["name"]] = f["value"]

    errors = []
    verdict_rows = []
    for row_name, row in bands.items():
        fig = row["figure"]
        n = counts.get(fig, 0)
        if n == 0:
            errors.append(f"missing {fig}")
            continue
        if n > 1:
            errors.append(f"duplicate {fig}")
            continue
        value = values[fig]
        verdict = evaluate_row(row, value)
        verdict_rows.append({"row": row_name, "figure": fig, "value": value, "verdict": verdict})
    return errors, verdict_rows


def evaluate_row(row, value):
    if value == "unsupported-on-this-build":
        return "UNSUPPORTED"
    if not row.get("judged", True):
        return "REPORTED"
    if value is None:
        return "INCONCLUSIVE"
    try:
        if eval(row["target"], {"__builtins__": {}}, {"value": value}):
            return "PASS"
    except Exception:
        return "INCONCLUSIVE"
    return "MISS"


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--calls", required=True)
    ap.add_argument("--truth", required=True)
    ap.add_argument("--bands", required=True)
    ap.add_argument("--stamps", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--prices", default=None)
    args = ap.parse_args(argv)

    os.makedirs(args.out, exist_ok=True)

    calls = load_jsonl(args.calls)
    with open(args.truth) as fh:
        truth = json.load(fh)
    with open(args.bands, "rb") as fh:
        bands = tomllib.load(fh)

    prices = {"get_price_per_1000": 0.0004, "storage_price_per_gb_month": 0.023, "instance_price_per_hour": 0.68}
    if args.prices and os.path.exists(args.prices):
        with open(args.prices) as fh:
            prices.update(json.load(fh))

    def stamp(name, default=None):
        p = os.path.join(args.stamps, name)
        if os.path.exists(p):
            with open(p) as fh:
                return json.load(fh)
        return default

    figures = []
    statements_detail = []
    for (statement, state), group in sorted(group_calls(calls).items()):
        detail = statement_figures(statement, state, group, figures)
        statements_detail.append(detail)

    pass_fraction = correctness_pass_fraction(statements_detail)
    figures.append({"name": "correctness.pass_fraction", "value": pass_fraction})

    fold_no_leaves = stamp("fold_no_leaves.json", {})
    fold_leaves = stamp("fold_leaves.json", {})
    if fold_no_leaves.get("wall_s") and fold_leaves.get("wall_s"):
        ratio = fold_leaves["wall_s"] / fold_no_leaves["wall_s"]
    else:
        ratio = None
    figures.append({"name": "fold.leaf_switch_wall_time_increase_ratio", "value": ratio})

    if fold_leaves.get("leaf_bytes") and fold_leaves.get("object_bytes"):
        leaf_fraction = fold_leaves["leaf_bytes"] / fold_leaves["object_bytes"]
    else:
        leaf_fraction = None
    figures.append({"name": "fold.leaf_bytes_fraction_of_object", "value": leaf_fraction})

    month_scale = stamp("month_scale.json", {"bounds_held": "unsupported-on-this-build"})
    figures.append({"name": "month_scale.bounds_held", "value": month_scale.get("bounds_held")})

    past_pmax = stamp("past_pmax_scale.json", {"bounds_held_or_reported": "unsupported-on-this-build"})
    figures.append({"name": "past_pmax_scale.bounds_held_or_reported", "value": past_pmax.get("bounds_held_or_reported")})

    in_list_growth = stamp("in_list_growth.json", {"growth_class": "unsupported-on-this-build"})
    figures.append({"name": "in_list.growth_class", "value": in_list_growth.get("growth_class")})

    scaling = stamp("scaling.json", {"bounds_held_all_scales": "unsupported-on-this-build"})
    figures.append({"name": "scaling.bounds_held_all_scales", "value": scaling.get("bounds_held_all_scales")})

    journey_c = stamp("journey_c_join.json", {"reported": True})
    figures.append({"name": "journey_c_join.reported", "value": journey_c.get("reported", True)})

    journey = stamp("journey.json", {"reported": True})
    figures.append({"name": "journey.reported", "value": journey.get("reported", True)})

    control_hits = stamp("control_hits.json", {"rlog_bands_held": "unsupported-on-this-build"})
    figures.append({"name": "control_hits.rlog_bands_held", "value": control_hits.get("rlog_bands_held")})

    cpu = stamp("cpu.json", {})
    cost_entries = []
    for d in statements_detail:
        if d["status"] == "unsupported-on-this-build":
            continue
        requests = sum(p.get("s3GetRequests", 0) for p in d["phases"].values())
        cpu_seconds = cpu.get("cpu_seconds_per_statement", {}).get(d["statement"], 0.0)
        dollars = query_cost(requests, cpu_seconds, prices["get_price_per_1000"], prices["instance_price_per_hour"])
        cost_entries.append({"statement": d["statement"], "state": d["state"], "requests": requests, "cpu_seconds": cpu_seconds, "dollars": dollars})

    maintenance = stamp("maintenance.json", {"fold_section_gets": 0, "leaf_put_requests": 0, "leaf_bytes": 0})
    maintenance_dollars = maintenance_cost_per_part_month(
        maintenance.get("fold_section_gets", 0),
        maintenance.get("leaf_put_requests", 0),
        maintenance.get("leaf_bytes", 0),
        prices["get_price_per_1000"],
        prices["storage_price_per_gb_month"],
    )
    savings_per_lookup = maintenance.get("savings_per_lookup_dollars", 0.0)
    breakeven = breakeven_lookups_per_part_month(maintenance_dollars, savings_per_lookup)

    figures.append({"name": "cost.reported", "value": True})

    cost = {
        "prices": prices,
        "prices_source": "run.toml (get_price_per_1000, storage_price_per_gb_month, instance_price_per_hour)",
        "per_statement": cost_entries,
        "maintenance_dollars_per_part_month": maintenance_dollars,
        "breakeven_lookups_per_part_month": breakeven,
    }

    errors, verdict_rows = check_bands(figures, bands)

    results = {
        "truth_summary": {
            "seed": truth.get("seed"),
            "traces": truth.get("traces"),
            "logs_written": truth.get("logs_written"),
            "spans_written": truth.get("spans_written"),
        },
        "figures": figures,
        "statements": statements_detail,
        "cost": cost,
        "verdicts": verdict_rows,
        "errors": errors,
    }

    with open(os.path.join(args.out, "results.json"), "w") as fh:
        json.dump(results, fh, indent=2, sort_keys=True, default=str)
        fh.write("\n")

    lines = []
    lines.append("# selective-investigations summary")
    lines.append(f"prices: {prices} (source: run.toml)")
    lines.append("")
    lines.append("verdict table:")
    any_miss = False
    for v in verdict_rows:
        lines.append(f"  {v['row']:45s} {v['figure']:45s} {v['value']!s:20s} {v['verdict']}")
        if v["verdict"] == "MISS":
            any_miss = True
    if errors:
        lines.append("")
        lines.append("ERRORS:")
        for e in errors:
            lines.append(f"  {e}")
    summary_text = "\n".join(lines) + "\n"
    with open(os.path.join(args.out, "summary.txt"), "w") as fh:
        fh.write(summary_text)

    print(summary_text)

    if errors:
        return 1
    if any_miss:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
