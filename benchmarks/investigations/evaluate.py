#!/usr/bin/env python3
"""Evaluate a selective-investigations run (issue #3699, ADR-3692 decision 8).

    evaluate.py --run DIR [--out-dir DIR] [--statements F] [--bands F]
                [--hits-truth F]

Reads, from the run directory box/run.sh wrote:

  run.toml       the effective run configuration: lane, states, cold runs,
                 prices and the query-frequency assumption
  truth.json     gen_corpus.py's oracle (answers by truth key)
  calls.jsonl    one line per call (box/lib/inv.py call / unsupported)
  stamps/        what the run proved about its own state (README.md "Stamps")

and writes results.<lane>.json and summary.<lane>.txt to --out-dir (default
the run directory). Output is a pure function of the inputs: no clock, no
absolute paths, sorted keys, so re-evaluating the same inputs is
byte-identical.

Every figure bands.toml names goes through one list, `figures`, and is
present exactly once: absent is `missing`, two inputs for it are
`duplicate` (never the last of them), a figure this build cannot produce is
`unsupported-on-this-build` with its reason, and a figure of a row whose
tenant, state, scale or baseline the run lacks is `not-run` with the reason.
Each answer is judged here against truth.json, under the truth key
statements.toml gives the call; a call whose rows differ from the oracle, or
that names another key, is `wrong_answer`. Exit 0 only when nothing is missing,
duplicated, wrong or outside its band; 1 otherwise; 64 on usage.
"""

import argparse
import json
import math
import os
import sys
import tomllib

HERE = os.path.dirname(os.path.abspath(__file__))
UNSUPPORTED = "unsupported-on-this-build"
PHASES = ("resolve", "plan", "probe", "scan", "keyIndex")
# leaf_rebuild is the folded_leaves state's `--rebuild-key-index` step: the
# leaves fold before it re-encodes nothing when the plain fold already sealed
# every hour.
FOLD_ORDER = ("folded_no_leaves", "folded_leaves", "leaf_rebuild", "fold_advance")
LEAF_TRANSITIONS = ("folded_leaves", "leaf_rebuild", "fold_advance")
SIGNALS = ("spans", "logs")
# The census class of each signal's own objects (docs/catalog-and-mvcc.md):
# t/<tenant_hash>/s/... and t/<tenant_hash>/l/...; leaves sit under catalog/.
SIGNAL_CLASS = {"spans": "s", "logs": "l"}

# A statement metric read from a nested stats member: absent from a call's
# stats means this build does not report it.
STATS_PATHS = {
    "leaf_gets": ("keyIndex", "leafGets"),
    "parts_covered": ("keyIndex", "partsCovered"),
    "keys_probed": ("keyIndex", "keysProbed"),
    "parts_beyond_ceiling": ("keyIndex", "partsBeyondCeiling"),
    "skip_idx_dropped": ("pruning", "skipIdxDropped"),
    "blocks_scanned": ("pruning", "blocksScanned"),
    "segments": ("pruning", "segments"),
    "dependency_depth": ("io", "dependencyDepth"),
}

# Fold report fields under their names in a `catalog fold --json` report and
# in a `--rebuild-key-index` report.
FOLD_FIELDS = {
    "key_index_ranged_gets": ("key_index_ranged_gets", "ranged_gets"),
    "leaf_bytes": ("key_index_leaf_bytes_written", "leaf_bytes_written"),
    "leaves_written": ("key_index_leaves_written", "leaves_written"),
}

PRICE_KEYS = ("get_per_1000", "put_per_1000", "storage_gb_month", "instance_per_hour")


class Problems:
    def __init__(self):
        self.lines = []

    def add(self, kind, what):
        self.lines.append(f"{kind} {what}")


def load_jsonl(path, problems, name):
    if not os.path.exists(path):
        problems.add("missing", name)
        return []
    out = []
    with open(path) as f:
        for n, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                problems.add("unreadable", f"{name}:{n}")
    return out


def load_json(path, problems, name):
    if not os.path.exists(path):
        problems.add("missing", name)
        return None
    with open(path) as f:
        try:
            return json.load(f)
        except ValueError:
            problems.add("unreadable", name)
            return None


def percentile(values, p):
    """Linear-interpolated percentile of a non-empty list."""
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def rnd(v):
    return round(v, 9) if isinstance(v, float) else v


# --- answers ------------------------------------------------------------------


def truth_key_of(call, statements, cold_runs):
    """The truth key statements.toml gives this call, or None for an unknown
    statement. A saved-trace statement's cold try i reads saved trace i and
    its warm repeat the last cold try's (box/lib/inv.py plan)."""
    sid = call.get("statement") or ""
    if sid.startswith("control_"):
        return "hits.control"
    st = statements.get(sid)
    if st is None:
        return None
    run = 0
    if st.get("per_run") == "saved_trace":
        run = call.get("try", 0) if call.get("temp", "cold") == "cold" else (cold_runs or 1) - 1
    return st["truth_key"].replace("{run}", str(run))


def judge(call, answers, key):
    """("correct" | "wrong_answer" | "unjudged", detail) for one ok call."""
    if key is None:
        return "wrong_answer", f"statement {call.get('statement')!r} not in statements.toml"
    if call.get("truth_key") != key:
        return "wrong_answer", f"call names truth key {call.get('truth_key')!r}, statements.toml gives {key!r}"
    ans = answers.get(key)
    if ans is None:
        return "wrong_answer", f"truth key {key!r} not in truth"
    if ans.get("oracle") == "none":
        return "unjudged", ans.get("reason", "no oracle")
    if call.get("rows") != ans["rows"]:
        return "wrong_answer", f"rows {call.get('rows')} expected {ans['rows']}"
    if "row_set" in ans:
        want = [str(v).lower() for v in ans["row_set"]]
        got = [str(v).lower() for v in call.get("values", [])]
        if "values" not in call or sorted(got) != sorted(want):
            return "wrong_answer", "row set differs from the oracle"
    if "value" in ans:
        if call.get("values") != [str(ans["value"])]:
            return "wrong_answer", f"value {call.get('values')} expected {ans['value']}"
    return "correct", ""


# --- per-call metrics ---------------------------------------------------------


def call_metrics(stats):
    """(metrics, unsupported reasons) of one call's stats member."""
    vals, unsup = {}, {}
    phases = {}
    if isinstance(stats.get("phases"), list):
        phases = {p.get("phase"): p for p in stats["phases"] if isinstance(p, dict)}
    else:
        unsup["phases"] = "stats carry no phases list"

    def ph(name, field):
        return int(phases.get(name, {}).get(field, 0) or 0)

    if phases:
        for p in PHASES:
            vals[f"{p.lower()}_gets"] = ph(p, "s3GetRequests")
            vals[f"{p.lower()}_wire_bytes"] = ph(p, "s3GetWireBytes")
        vals["total_gets"] = sum(ph(p, "s3GetRequests") for p in phases)
        vals["data_gets"] = ph("probe", "s3GetRequests") + ph("scan", "s3GetRequests")
        vals["list_requests"] = sum(ph(p, "s3ListRequests") for p in phases)
        vals["wire_bytes"] = sum(ph(p, "s3GetWireBytes") for p in phases)
        vals["decompressed_bytes"] = sum(ph(p, "decompressedOutputBytes") for p in phases)
        vals["segments_opened"] = sum(ph(p, "segmentsOpened") for p in phases)
    else:
        for name in ("total_gets", "data_gets", "list_requests", "wire_bytes", "decompressed_bytes",
                     "segments_opened", "keyindex_gets", "scan_gets", "probe_gets", "resolve_gets",
                     "scan_wire_bytes", "probe_wire_bytes"):
            unsup[name] = "stats carry no phases list"
    for name, (member, field) in STATS_PATHS.items():
        obj = stats.get(member)
        if isinstance(obj, dict) and field in obj:
            vals[name] = obj[field]
        else:
            unsup[name] = f"stats carry no {member}.{field}"
    if isinstance(stats.get("signals"), list):
        vals["signals_entries"] = len(stats["signals"])
    else:
        unsup["signals_entries"] = "stats carry no signals list"
    return vals, unsup


# --- the run's calls ----------------------------------------------------------


class Calls:
    def __init__(self, calls, answers, problems, statements, cold_runs):
        self.groups = {}  # (statement, state, temp) -> {try: [lines]}
        self.unsupported = {}  # (statement, state, temp) -> reason
        self.wrong = []
        self.errors = []
        self.unjudged = 0
        for c in calls:
            key = (c.get("statement"), c.get("state"), c.get("temp", "cold"))
            self.groups.setdefault(key, {}).setdefault(c.get("try", 0), []).append(c)
        for key, tries in sorted(self.groups.items()):
            for t, lines in sorted(tries.items()):
                if len(lines) > 1:
                    problems.add("duplicate", f"call {key[0]}/{key[1]}/{key[2]} try {t}")
                for c in lines:
                    if c.get("outcome") == UNSUPPORTED:
                        self.unsupported[key] = c.get("reason") or "no reason given"
                        if not c.get("reason"):
                            problems.add("missing", f"reason for unsupported {key[0]}/{key[1]}")
                    elif c.get("outcome") == "ok":
                        verdict, detail = judge(c, answers, truth_key_of(c, statements, cold_runs))
                        c["_judged"] = verdict
                        if verdict == "wrong_answer":
                            self.wrong.append(f"{key[0]}/{key[1]}/{key[2]} try {t}: {detail}")
                        elif verdict == "unjudged":
                            self.unjudged += 1
                    else:
                        c["_judged"] = "error"
                        self.errors.append(f"{key[0]}/{key[1]}/{key[2]} try {t}: HTTP {c.get('http_status')}")

    def tries(self, statement, state, temp):
        return self.groups.get((statement, state, temp), {})


def expected_tries(temp, cfg):
    return cfg["cold_runs"] if temp == "cold" else cfg["warm_runs"]


def statement_figure(calls, cfg, statement, state, metric):
    """(status, value, reason) of one statement figure.

    metric is "<temp>.<agg>.<name>", "<temp>.rows_exact",
    "<temp>.rows_match_state.<other>" or "<temp>.wall_p50_s".
    """
    temp, _, rest = metric.partition(".")
    key = (statement, state, temp)
    if key in calls.unsupported:
        return UNSUPPORTED, None, calls.unsupported[key]
    tries = calls.tries(statement, state, temp)
    if not tries:
        return "missing", None, f"no {temp} call of {statement} in {state}"
    if any(len(v) > 1 for v in tries.values()):
        return "duplicate", None, f"two calls for one {temp} try of {statement} in {state}"
    want = expected_tries(temp, cfg)
    absent = [t for t in range(want) if t not in tries]
    if absent:
        return "missing", None, f"{temp} tries {absent} of {statement} in {state}"
    extra = sorted(t for t in tries if t >= want)
    if extra:
        return "duplicate", None, f"{temp} tries {extra} beyond the {want} the run declares"
    lines = [tries[t][0] for t in range(want)]
    if rest == "rows_exact":
        return "value", int(all(c.get("_judged") == "correct" for c in lines)), ""
    if rest.startswith("rows_match_state."):
        other = rest.split(".", 1)[1]
        theirs = calls.tries(statement, other, temp)
        if not theirs or any(len(theirs.get(t, [])) != 1 for t in range(want)):
            return "missing", None, f"{temp} calls of {statement} in {other} to compare against"
        same = all(
            c.get("outcome") == "ok"
            and theirs[t][0].get("outcome") == "ok"
            and c.get("rows") == theirs[t][0].get("rows")
            and c.get("values", c.get("values_sha256")) == theirs[t][0].get("values", theirs[t][0].get("values_sha256"))
            for t, c in enumerate(lines)
        )
        return "value", int(same), ""
    if any(c.get("outcome") != "ok" for c in lines):
        return "missing", None, f"a {temp} call of {statement} in {state} failed"
    if rest == "wall_p50_s":
        return "value", rnd(percentile([c["wall_s"] for c in lines], 0.5)), ""
    agg, _, name = rest.partition(".")
    if name.startswith("per_object."):
        return UNSUPPORTED, None, "stats carry no per-object breakdown"
    vals = []
    for c in lines:
        if not isinstance(c.get("stats"), dict):
            return "missing", None, f"{temp} try {c.get('try')} of {statement} in {state} has no stats (RQ_STATS unset?)"
        v, unsup = call_metrics(c["stats"])
        if name in unsup:
            return UNSUPPORTED, None, unsup[name]
        if name not in v:
            return "missing", None, f"unknown metric {name}"
        vals.append(v[name])
    return "value", (max(vals) if agg == "max" else min(vals)), ""


# --- folds --------------------------------------------------------------------


def index_lines(lines, key_fields, name, problems):
    out = {}
    for line in lines:
        k = tuple(line.get(f) for f in key_fields)
        if k in out:
            problems.add("duplicate", f"{name} {'/'.join(str(x) for x in k)}")
            out[k] = None
        else:
            out[k] = line
    return out


def fold_field(report, field):
    for name in FOLD_FIELDS.get(field, (field,)):
        if name in report:
            return report[name]
    return None


def rebuild_requests(report):
    """(GETs, PUTs) a `--rebuild-key-index` report counts: per attempt, the
    part GETs and ranged section GETs, and the leaf PUTs plus one HEAD publish
    per CAS try. It counts no HEAD read and no listing."""
    attempts = report.get("attempts") or []
    gets = sum(int(a.get("part_gets", 0)) + int(a.get("ranged_gets", 0)) for a in attempts)
    puts = sum(int(a.get("leaf_puts", 0)) + int(a.get("head_cas_tries", 0)) for a in attempts)
    return gets, puts


def fold_figure(folds, census, transition, signal, field):
    """(status, value, reason) of fold.<signal>.<field> for one transition."""
    line = folds.get((transition, signal), "absent")
    if line == "absent":
        return "missing", None, f"no fold stamp for {transition}/{signal}"
    if line is None:
        return "duplicate", None, f"two fold stamps for {transition}/{signal}"
    report = line.get("report") or {}
    if field == "peak_rss_bytes":
        v = (line.get("measure") or {}).get("peak_rss_bytes")
        return ("value", v, "") if v is not None else ("missing", None, "fold stamp has no measure")
    earlier = [t for t in FOLD_ORDER[: FOLD_ORDER.index(transition)]] if transition in FOLD_ORDER else []
    if field == "new_entries":
        prev = [folds.get((t, signal)) for t in earlier]
        prev = [p for p in prev if p not in (None, "absent") and "entry_count" in (p.get("report") or {})
                and not p["report"].get("no_op")]
        if "entry_count" not in report:
            return UNSUPPORTED, None, "fold report carries no entry_count"
        if not prev:
            return "missing", None, f"no earlier fold of {signal} to difference entry_count against"
        return "value", report["entry_count"] - prev[-1]["report"]["entry_count"], ""
    if field == "previous_leaves":
        total = 0
        for t in earlier:
            p = folds.get((t, signal))
            if p in (None, "absent"):
                continue
            total += fold_field(p.get("report") or {}, "leaves_written") or 0
        return "value", total, ""
    if field == "leaf_bytes_pct":
        leaf = fold_field(report, "leaf_bytes")
        if leaf is None:
            return UNSUPPORTED, None, "fold report carries no leaf bytes"
        # The fold report's part_bytes is the snapshot part, not the data it
        # indexes: the denominator is the signal's own objects in the census
        # of the state the leaves were built for.
        state = "folded_leaves" if transition == "leaf_rebuild" else transition
        line = census.get((state,), "absent")
        if line in (None, "absent"):
            return "missing", None, f"no census of {state} for the {signal} data bytes"
        data = ((line.get("census") or {}).get("classes") or {}).get(SIGNAL_CLASS[signal], {}).get("bytes")
        if not data:
            return "missing", None, f"census of {state} has no {SIGNAL_CLASS[signal]}/ bytes for {signal}"
        return "value", rnd(100.0 * leaf / data), ""
    v = fold_field(report, field)
    if v is None:
        return UNSUPPORTED, None, f"fold report carries no {field}"
    return "value", v, ""


# --- rows ---------------------------------------------------------------------


def row_not_run(row, cfg):
    if row.get("requires_tenant") == "hits" and not cfg["hits"]:
        return "the hits tenant is not loaded (run.toml hits = false)"
    lacking = [s for s in row.get("requires_states", []) if s not in cfg["states"]]
    if lacking:
        return f"states {lacking} not in run.toml states"
    scales = [s for s in row.get("requires_scales", []) if s not in cfg["scales"]]
    if scales:
        return f"scales {scales} not in run.toml scales"
    if row.get("requires_baseline") and not cfg["baseline"]:
        return "no baseline build (run.toml baseline = false)"
    return None


def evaluate_expr(expr, v, m):
    return bool(eval(expr, {"__builtins__": {}}, {"v": v, "m": m}))  # noqa: S307 - bands.toml is repo-controlled


def run_rows(bands, cfg, calls, folds, census, matrix, cost_figs):
    figures = []
    for row in bands["row"]:
        reason = row_not_run(row, cfg)
        for fig in row["figure"]:
            fid = f"{row['key']}.{fig['name']}"
            entry = {"id": fid, "row": row["adr_row"], "statement": fig["statement"], "state": fig["state"],
                     "metric": fig["metric"], "lane": fig["lane"]}
            metric = fig["metric"]
            if reason:
                status, value, why = "not-run", None, reason
            elif fig["state"] in cfg["unsupported_states"]:
                status, value, why = UNSUPPORTED, None, cfg["unsupported_states"][fig["state"]]
            elif metric.startswith("fold."):
                _, signal, field = metric.split(".", 2)
                status, value, why = fold_figure(folds, census, fig.get("transition", fig["state"]), signal, field)
            elif metric.startswith("matrix."):
                status, value, why = matrix[metric]
            elif metric.startswith("cost."):
                status, value, why = "value", cost_figs[metric], ""
            elif metric.startswith(("scale.", "control.")):
                status, value, why = "missing", None, f"{metric} needs a run this harness lane does not perform"
            else:
                status, value, why = statement_figure(calls, cfg, fig["statement"], fig["state"], metric)
            entry.update(status=status, value=value, reason=why)
            figures.append(entry)
    by_id = {f["id"]: f["value"] for f in figures if f["status"] == "value"}
    for row, f in ((r, f) for r in bands["row"] for f in r["figure"]):
        entry = next(e for e in figures if e["id"] == f"{row['key']}.{f['name']}")
        if entry["status"] != "value":
            entry["verdict"] = entry["status"]
            continue
        if f["lane"] == "timing" and cfg["lane"] != "s3":
            entry["verdict"] = "reported (timing is judged on the s3 lane only)"
            continue
        try:
            ok = evaluate_expr(f.get("target_if", "True"), entry["value"], by_id)
            miss = evaluate_expr(f.get("miss_if", "False"), entry["value"], by_id)
        except KeyError as e:
            entry["verdict"] = "missing"
            entry["reason"] = f"band refers to figure {e.args[0]} that has no value"
            continue
        entry["verdict"] = "pass" if ok and not miss else "miss"
    return figures


# --- cost ---------------------------------------------------------------------


def price_basis(cfg):
    prices = cfg["prices"]
    basis = {}
    for k in PRICE_KEYS:
        basis[k] = {"value": prices.get(k), "source": prices.get(f"{k}_source")}
    return basis


def statement_cost(calls, cfg):
    """Dollars per cold call of every (statement, state) with clean ok calls."""
    p = cfg["prices"]
    out = []
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        lines = [v[0] for v in tries.values() if len(v) == 1 and v[0].get("outcome") == "ok"]
        if temp != "cold" or not lines or len(lines) != len(tries):
            continue
        gets, lists, cpu = [], [], []
        for c in lines:
            if not isinstance(c.get("stats"), dict):
                break
            v, _ = call_metrics(c["stats"])
            if "total_gets" not in v:
                break
            gets.append(v["total_gets"])
            lists.append(v["list_requests"])
            cpu.append(c.get("server_cpu_s", 0.0))
        else:
            n = len(lines)
            g, li, s = sum(gets) / n, sum(lists) / n, sum(cpu) / n
            dollars = g * p["get_per_1000"] / 1000 + li * p["put_per_1000"] / 1000 + s * p["instance_per_hour"] / 3600
            out.append({
                "statement": statement, "state": state, "calls": n,
                "mean_gets": rnd(g), "mean_list_requests": rnd(li), "mean_server_cpu_s": rnd(s),
                "server_cpu_s_p50": rnd(percentile(cpu, 0.5)),
                "server_hwm_bytes_max": max((c.get("server_hwm_bytes") or 0) for c in lines),
                "dollars_per_call": rnd(dollars),
                "basis": "get_per_1000, put_per_1000 (LIST), instance_per_hour",
            })
    return out


def maintenance_cost(folds, census, cfg):
    """Each fold transition's dollars, from its report and measured CPU."""
    p = cfg["prices"]
    out = []
    for (transition, signal), line in sorted(folds.items()):
        if not line:
            continue
        r = line.get("report") or {}
        if "get_requests" in r:
            gets, puts, lists = r["get_requests"], r.get("put_requests", 0), r.get("list_requests", 0)
        else:
            (gets, puts), lists = rebuild_requests(r), 0
        cpu = (line.get("measure") or {}).get("cpu_s", 0.0)
        leaf = fold_field(r, "leaf_bytes") or 0
        dollars = gets * p["get_per_1000"] / 1000 + (puts + lists) * p["put_per_1000"] / 1000 + cpu * p["instance_per_hour"] / 3600
        out.append({
            "transition": transition, "signal": signal, "kind": line.get("kind", "fold"),
            "get_requests": gets, "put_requests": puts, "list_requests": lists, "cpu_s": cpu,
            "leaf_bytes": leaf, "parts_total": r.get("parts_total", r.get("parts_rebuilt")),
            "dollars": rnd(dollars),
            "leaf_storage_dollars_per_month": rnd(leaf / 1e9 * p["storage_gb_month"]),
            "basis": "get_per_1000, put_per_1000 (PUT and LIST), instance_per_hour, storage_gb_month",
        })
    storage = []
    for (state,), line in sorted(census.items()):
        if not line:
            continue
        b = (line.get("census") or {}).get("bytes", 0)
        storage.append({"state": state, "objects": (line.get("census") or {}).get("objects"), "bytes": b,
                        "dollars_per_month": rnd(b / 1e9 * p["storage_gb_month"]), "basis": "storage_gb_month"})
    return out, storage


def break_even(stmt_cost, maint, cfg):
    """Per lookup statement: saving per call of folded_leaves over unfolded,
    and the lookups per part-month the leaf build needs to pay for itself."""
    c = cfg["cost"]
    freq = c.get("lookups_per_part_month")
    months = c.get("amortise_months", 1)
    leaf_folds = [m for m in maint if m["transition"] in LEAF_TRANSITIONS]
    leaf_dollars = sum(m["dollars"] for m in leaf_folds) / months + sum(m["leaf_storage_dollars_per_month"] for m in leaf_folds)
    parts = max([m["parts_total"] or 0 for m in leaf_folds] or [0]) or 1
    per_part_month = leaf_dollars / parts
    by = {(s["statement"], s["state"]): s for s in stmt_cost}
    out = []
    for (statement, state), s in sorted(by.items()):
        if state != "folded_leaves" or (statement, "unfolded") not in by:
            continue
        saving = by[(statement, "unfolded")]["dollars_per_call"] - s["dollars_per_call"]
        entry = {"statement": statement, "dollars_unfolded": by[(statement, "unfolded")]["dollars_per_call"],
                 "dollars_folded_leaves": s["dollars_per_call"], "saving_per_call": rnd(saving),
                 "maintenance_per_part_month": rnd(per_part_month), "parts": parts,
                 "lookups_per_part_month_assumed": freq, "amortise_months": months}
        if saving > 0:
            entry["break_even_lookups_per_part_month"] = rnd(per_part_month / saving)
            if freq is not None:
                entry["net_per_part_month_at_assumed_frequency"] = rnd(freq * saving - per_part_month)
        else:
            entry["break_even_lookups_per_part_month"] = "never (no saving per call)"
        out.append(entry)
    return out


# --- stamps -------------------------------------------------------------------


def required_stamps(run_dir, cfg, problems):
    stamps = os.path.join(run_dir, "stamps")
    docs = {}
    for name in ("build.json", "host.json", "declare.json"):
        docs[name] = load_json(os.path.join(stamps, name), problems, f"stamps/{name}")
    loads = index_lines(load_jsonl(os.path.join(stamps, "loads.jsonl"), problems, "stamps/loads.jsonl"),
                        ("hour", "signal"), "stamps/loads.jsonl", problems)
    folds = index_lines(load_jsonl(os.path.join(stamps, "folds.jsonl"), problems, "stamps/folds.jsonl"),
                        ("transition", "signal"), "stamps/folds.jsonl", problems)
    census = index_lines(load_jsonl(os.path.join(stamps, "census.jsonl"), problems, "stamps/census.jsonl"),
                         ("state",), "stamps/census.jsonl", problems)
    restarts = index_lines(load_jsonl(os.path.join(stamps, "restarts.jsonl"), problems, "stamps/restarts.jsonl"),
                           ("statement", "state", "try"), "stamps/restarts.jsonl", problems)
    transitions = index_lines(load_jsonl(os.path.join(stamps, "transitions.jsonl"), problems,
                                         "stamps/transitions.jsonl"), ("state",), "stamps/transitions.jsonl", problems)
    live = [s for s in cfg["states"] if s not in cfg["unsupported_states"]]
    hours = ["base", "extra"] if "mixed" in live else ["base"]
    for hour in hours:
        for sig in SIGNALS:
            if (hour, sig) not in loads:
                problems.add("missing", f"stamps/loads.jsonl {hour}/{sig}")
            elif loads[(hour, sig)]:
                line = loads[(hour, sig)]
                if line.get("exit") != 0:
                    problems.add("failed", f"load {hour}/{sig} exit {line.get('exit')}")
    # fold_advance and leaf_rebuild are no query states: the first is the fold
    # that follows the mixed state's extra hour, the second a step of
    # folded_leaves, so those states being live is what requires them.
    for t in FOLD_ORDER:
        if (t in live or (t == "fold_advance" and "mixed" in live)
                or (t == "leaf_rebuild" and "folded_leaves" in live)):
            for sig in SIGNALS:
                if (t, sig) not in folds:
                    problems.add("missing", f"stamps/folds.jsonl {t}/{sig}")
                elif folds[(t, sig)] and folds[(t, sig)].get("exit") != 0:
                    problems.add("failed", f"fold {t}/{sig} exit {folds[(t, sig)].get('exit')}")
    for s in live:
        if (s,) not in census:
            problems.add("missing", f"stamps/census.jsonl {s}")
        if (s,) not in transitions:
            problems.add("missing", f"stamps/transitions.jsonl {s}")
    return docs, loads, folds, census, restarts


def check_loads(loads, truth, problems):
    rows = (truth or {}).get("rows_written", {})
    for sig in SIGNALS:
        line = loads.get(("base", sig))
        if not line or sig not in rows:
            continue
        report = line.get("report") or {}
        # This build's load report counts objects, not rows; the row count is
        # then proven by the statements judged against truth.json.
        if "rows_written" in report:
            if report["rows_written"] != rows[sig]:
                problems.add("wrong", f"load base/{sig} wrote {report['rows_written']} rows, "
                                      f"truth.json says {rows[sig]}")
        elif not report.get("objects_written"):
            problems.add("wrong", f"load base/{sig} report shows no objects_written")


def check_restarts(calls, restarts, problems):
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        if temp != "cold" or (statement, state, temp) in calls.unsupported:
            continue
        for t in sorted(tries):
            line = restarts.get((statement, state, t), "absent")
            if line == "absent":
                problems.add("missing", f"stamps/restarts.jsonl {statement}/{state}/{t}")


# --- main ---------------------------------------------------------------------


def load_cfg(run_dir, problems):
    path = os.path.join(run_dir, "run.toml")
    if not os.path.exists(path):
        problems.add("missing", "run.toml")
        return None
    with open(path, "rb") as f:
        doc = tomllib.load(f)
    run = doc.get("run", {})
    cfg = {
        "lane": run.get("lane"),
        "states": run.get("states", []),
        "unsupported_states": doc.get("unsupported_states", {}),
        "cold_runs": run.get("cold_runs"),
        "warm_runs": run.get("warm_runs", 1),
        "hits": run.get("hits", False),
        "scales": run.get("scales", [1]),
        "baseline": run.get("baseline", False),
        "prices": doc.get("prices", {}),
        "cost": doc.get("cost", {}),
    }
    for k in ("lane", "cold_runs"):
        if cfg[k] is None:
            problems.add("missing", f"run.toml run.{k}")
    for k in PRICE_KEYS:
        if not isinstance(cfg["prices"].get(k), (int, float)):
            problems.add("missing", f"run.toml prices.{k}")
            cfg["prices"][k] = 0.0
    return cfg


def summary_text(res):
    out = [f"selective investigations, lane {res['lane']}: {res['verdict']}", ""]
    out.append(f"{'figure':44} {'status':12} {'value':>14}  verdict")
    for f in res["figures"]:
        v = "" if f["value"] is None else str(f["value"])
        out.append(f"{f['id']:44} {f['status'][:12]:12} {v:>14}  {f['verdict']}"
                   + (f"  ({f['reason']})" if f["reason"] else ""))
    out.append("")
    out.append("statement                       state             p50_s     p95_s     p99_s  gets  wire_B   cpu_s   $/call")
    for s in res["statements"]:
        if s.get("status") == UNSUPPORTED:
            out.append(f"{s['statement'][:31]:31} {s['state'][:17]:17} {UNSUPPORTED} ({s['reason']})")
            continue
        out.append(f"{s['statement'][:31]:31} {s['state'][:17]:17} {s['wall_p50_s']:>7} {s['wall_p95_s']:>9} "
                   f"{s['wall_p99_s']:>9} {s.get('max_total_gets', '')!s:>5} {s.get('max_wire_bytes', '')!s:>7} "
                   f"{s['server_cpu_s_p50']:>7} {s.get('dollars_per_call', '')!s:>8}")
    out.append("")
    out.append("prices:")
    for k, b in res["cost"]["prices"].items():
        out.append(f"  {k} = {b['value']}  source: {b['source']}")
    c = res["cost"]
    out.append(f"query frequency assumed: {c['lookups_per_part_month_assumed']} lookups per part-month"
               f" (source: {c['lookups_per_part_month_source']}); leaf build amortised over {c['amortise_months']} month(s)")
    for b in c["break_even"]:
        out.append(f"  {b['statement']}: unfolded ${b['dollars_unfolded']} folded ${b['dollars_folded_leaves']} "
                   f"saving ${b['saving_per_call']}/call; maintenance ${b['maintenance_per_part_month']}/part-month; "
                   f"break-even {b['break_even_lookups_per_part_month']} lookups/part-month")
    for m in c["maintenance"]:
        out.append(f"  fold {m['transition']}/{m['signal']} ({m['kind']}): {m['get_requests']} GET {m['put_requests']} PUT "
                   f"{m['list_requests']} LIST {m['cpu_s']} cpu_s = ${m['dollars']}")
    if res["problems"]:
        out.append("")
        out.extend(res["problems"])
    return "\n".join(out) + "\n"


def statement_detail(calls):
    out = []
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        lines = [v[0] for _, v in sorted(tries.items())]
        if (statement, state, temp) in calls.unsupported:
            out.append({"statement": statement, "state": state, "temp": temp, "status": UNSUPPORTED,
                        "reason": calls.unsupported[(statement, state, temp)]})
            continue
        ok = [c for c in lines if c.get("outcome") == "ok"]
        walls = [c["wall_s"] for c in ok] or [0.0]
        cpu = [c.get("server_cpu_s", 0.0) for c in ok] or [0.0]
        d = {"statement": statement, "state": state, "temp": temp, "calls": len(lines),
             "ok": len(ok), "correct": sum(1 for c in ok if c.get("_judged") == "correct"),
             "wall_p50_s": rnd(percentile(walls, 0.5)), "wall_p95_s": rnd(percentile(walls, 0.95)),
             "wall_p99_s": rnd(percentile(walls, 0.99)), "server_cpu_s_p50": rnd(percentile(cpu, 0.5)),
             "server_hwm_bytes_max": max((c.get("server_hwm_bytes") or 0) for c in lines)}
        mets = [call_metrics(c["stats"])[0] for c in ok if isinstance(c.get("stats"), dict)]
        for name in ("total_gets", "data_gets", "resolve_gets", "probe_gets", "scan_gets", "keyindex_gets",
                     "list_requests", "wire_bytes", "decompressed_bytes", "segments_opened", "blocks_scanned",
                     "segments", "dependency_depth", "signals_entries"):
            vs = [m[name] for m in mets if name in m]
            if vs and len(vs) == len(ok):
                d[f"max_{name}"] = max(vs)
        out.append(d)
    return out


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--out-dir")
    ap.add_argument("--statements", default=os.path.join(HERE, "statements.toml"))
    ap.add_argument("--bands", default=os.path.join(HERE, "bands.toml"))
    ap.add_argument("--hits-truth", default=os.path.join(HERE, "hits.truth.json"))
    args = ap.parse_args(argv)
    if not os.path.isdir(args.run):
        print(f"evaluate.py: no run directory {args.run}", file=sys.stderr)
        return 64
    out_dir = args.out_dir or args.run
    problems = Problems()
    cfg = load_cfg(args.run, problems)
    truth = load_json(os.path.join(args.run, "truth.json"), problems, "truth.json")
    calls_raw = load_jsonl(os.path.join(args.run, "calls.jsonl"), problems, "calls.jsonl")
    if cfg is None:
        print("\n".join(problems.lines))
        return 1
    with open(args.bands, "rb") as f:
        bands = tomllib.load(f)
    with open(args.statements, "rb") as f:
        statements = {s["id"]: s for s in tomllib.load(f)["statement"]}
    answers = dict((truth or {}).get("answers", {}))
    if cfg["hits"]:
        with open(args.hits_truth) as f:
            answers.update(json.load(f))
    calls = Calls(calls_raw, answers, problems, statements, cfg["cold_runs"])
    docs, loads, folds, census, restarts = required_stamps(args.run, cfg, problems)
    check_loads(loads, truth, problems)
    check_restarts(calls, restarts, problems)
    for w in calls.wrong:
        problems.add("wrong_answer", w)
    for e in calls.errors:
        problems.add("error", e)

    mixed = [(k, v) for k, v in calls.groups.items() if k[1] == "mixed" and k not in calls.unsupported]
    matrix = {
        "matrix.wrong_answers": ("value", len(calls.wrong) + len(calls.errors), ""),
        "matrix.state_rows_exact": (
            ("value", int(all(c[0].get("_judged") in ("correct", "unjudged") for _, t in mixed for c in t.values())), "")
            if mixed else ("missing", None, "no calls in mixed")),
        "matrix.negative_controls": (UNSUPPORTED, None, "the decision 9 mutants are not built or run by this harness"),
    }
    stmt_cost = statement_cost(calls, cfg)
    maint, storage = maintenance_cost(folds, census, cfg)
    be = break_even(stmt_cost, maint, cfg)
    basis = price_basis(cfg)
    no_basis = [k for k, b in basis.items() if not b["source"]]
    cost_figs = {
        "cost.figures_without_basis": (len(stmt_cost) + len(maint) + len(storage)) if no_basis else 0,
        "cost.statement_figures": len(stmt_cost),
        "cost.break_even_without_frequency": (
            len(be) if cfg["cost"].get("lookups_per_part_month") is None
            or not cfg["cost"].get("lookups_per_part_month_source") else 0),
    }
    figures = run_rows(bands, cfg, calls, folds, census, matrix, cost_figs)
    for f in figures:
        if f["verdict"] in ("missing", "duplicate", "miss"):
            problems.add(f["verdict"], f"{f['id']}" + (f": {f['reason']}" if f["reason"] else ""))
    detail = statement_detail(calls)
    costs = {(s["statement"], s["state"]): s for s in stmt_cost}
    for d in detail:
        c = costs.get((d["statement"], d["state"])) if d["temp"] == "cold" else None
        if c:
            d["dollars_per_call"] = c["dollars_per_call"]
            d["mean_server_cpu_s"] = c["mean_server_cpu_s"]
    verdict = "fail" if problems.lines else "pass"
    res = {
        "lane": cfg["lane"],
        "verdict": verdict,
        "problems": problems.lines,
        "figures": figures,
        "statements": detail,
        "unjudged_calls": calls.unjudged,
        "cost": {
            "prices": basis,
            "statements": stmt_cost,
            "maintenance": maint,
            "storage": storage,
            "break_even": be,
            "lookups_per_part_month_assumed": cfg["cost"].get("lookups_per_part_month"),
            "lookups_per_part_month_source": cfg["cost"].get("lookups_per_part_month_source"),
            "amortise_months": cfg["cost"].get("amortise_months", 1),
        },
        "stamps": {
            "build": docs.get("build.json"),
            "host": docs.get("host.json"),
            "declare": docs.get("declare.json"),
            "loads": [v for _, v in sorted(loads.items()) if v],
            "folds": [{k: v for k, v in line.items() if k != "stdout"} for _, line in sorted(folds.items()) if line],
        },
    }
    os.makedirs(out_dir, exist_ok=True)
    lane = cfg["lane"] or "unknown"
    with open(os.path.join(out_dir, f"results.{lane}.json"), "w") as f:
        json.dump(res, f, indent=1, sort_keys=True)
        f.write("\n")
    text = summary_text(res)
    with open(os.path.join(out_dir, f"summary.{lane}.txt"), "w") as f:
        f.write(text)
    sys.stdout.write(text)
    return 1 if problems.lines else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
