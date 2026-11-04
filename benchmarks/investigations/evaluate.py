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
Only the fields ALLOWLISTED below may read as unsupported when a successful
call lacks them; any other absent field is `missing`, and no figure
defaults to 0. Each answer is judged here against truth.json, under the
truth key statements.toml gives the call, or, in the states whose oracle is
the unfolded run, against the unfolded call of the same statement and try;
a call whose rows differ from its oracle, or that names another key, is
`wrong_answer`. A state run.sh recorded failed (stamps/failed_states.jsonl:
a load, fold, leaf rebuild, compaction or erasure command of it exited
non-zero) is not live: every figure of it, and of a state derived from it,
is `not-run` with the reason "state failed at <step> (exit N)", and its
entry in `states` reads `failed` with the step, exit code and log. The
verdict is `fail` on any problem; else `incomplete` when an asked state, a
planned call or an ungated figure did not run; else `pass`. Exit 0 only on pass; 1 on fail or incomplete; 64 on usage.
"""

import argparse
import json
import math
import os
import re
import sys
import tomllib

HERE = os.path.dirname(os.path.abspath(__file__))
UNSUPPORTED = "unsupported-on-this-build"
# A server refusal of a statement statements.toml marks
# expect_refusal_on_main: the SQL complexity guard working as designed.
GUARD = "refused-by-guard"
PHASES = ("resolve", "plan", "probe", "scan", "keyIndex")
PHASE_FIELDS = ("s3GetRequests", "s3GetWireBytes", "s3ListRequests", "segmentsOpened",
                "decompressedOutputBytes", "cacheServedBytes")
# States whose oracle is the unfolded state's own answer to the same
# statement and try: their data is the unfolded data mid-rewrite.
ORACLE_STATES = ("compaction_in_flight", "compaction_done", "erasure_in_flight")
# States run.sh enters as a step of another one: live exactly when it is.
DERIVED = {"fold_advance": "mixed", "compaction_done": "compaction_in_flight"}
INFLIGHT_STATES = ("compaction_in_flight",)
SIGNALS = ("spans", "logs")

# The only stats fields this build does not report. A successful call lacking
# one of them reads unsupported-on-this-build; lacking anything else, missing.
KEY_INDEX_METRICS = {
    "leaf_gets": "leafGetRequests",
    "parts_covered": "partsCovered",
    "parts_uncovered": "partsUncovered",
    "parts_beyond_ceiling": "partsBeyondCeiling",
    "leaves_rejected": "leavesRejected",
    "reservations_refused": "reservationsRefused",
    "keys_probed": "keysProbed",
}
ALLOWLIST_BREAKDOWNS = {"per_object": "stats carry no per-object breakdown",
                        "per_part": "stats carry no per-part breakdown"}
# Fold report fields this build does not write.
FOLD_ALLOWLIST = ("key_index_build_passes", "head_bytes_per_part_field")

# Fold report fields under their names in a `catalog fold --json` report and
# in a `--rebuild-key-index` report.
FOLD_FIELDS = {
    "key_index_ranged_gets": ("key_index_ranged_gets", "ranged_gets"),
    "leaf_bytes": ("key_index_leaf_bytes_written", "leaf_bytes_written"),
    "leaves_written": ("key_index_leaves_written", "leaves_written"),
}

PRICE_KEYS = ("get_per_1000", "put_per_1000", "storage_gb_month", "instance_per_hour")
NEEDLE_RARE = "needlerare"
# (truth needle_blocks key, logs column, band): the rare needle exact, the
# sparse and common needles within NEEDLE_BLOCK_BAND, tag (every needle,
# truth "any") reported only.
NEEDLE_GEOMETRY = (
    (NEEDLE_RARE, "needle_rare", "exact"),
    ("needlesparse", "needle_sparse", "band"),
    ("needlecommon", "needle_common", "band"),
    ("any", "tag", "reported"),
)
NEEDLE_BLOCK_BAND = 0.25
# inv.py window_bounds' windows.
HOUR_NS = 3_600_000_000_000
NARROW_NS = 300_000_000_000


class Problems:
    def __init__(self):
        self.lines = []

    def add(self, kind, what):
        line = f"{kind} {what}"
        if line not in self.lines:
            self.lines.append(line)


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


def where(c):
    return f"{c.get('statement')}/{c.get('state')}/{c.get('temp', 'cold')} try {c.get('try', 0)}"


# --- the run's configuration and states ---------------------------------------


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
        "cold_runs": run.get("cold_runs"),
        "warm_runs": run.get("warm_runs", 1),
        "hits": run.get("hits", False),
        "scales": run.get("scales", [1]),
        "baseline": run.get("baseline", False),
        "tenant": doc.get("server", {}).get("tenant", "inv"),
        "prices": doc.get("prices", {}),
        "cost": doc.get("cost", {}),
    }
    for k in ("lane", "cold_runs"):
        if cfg[k] is None:
            problems.add("missing", f"run.toml run.{k}")
    if cfg["cold_runs"] is None:
        cfg["cold_runs"] = 0
    for k in PRICE_KEYS:
        if not isinstance(cfg["prices"].get(k), (int, float)):
            problems.add("missing", f"run.toml prices.{k}")
            cfg["prices"][k] = 0.0
    months = cfg["cost"].get("amortise_months")
    if isinstance(months, bool) or not isinstance(months, int) or months < 1:
        problems.add("missing", "run.toml cost.amortise_months (a whole number of months, at least 1)")
        cfg["cost"]["amortise_months"] = None
    return cfg


def failed_states(run_dir, problems):
    """{state: its first stamps/failed_states.jsonl line}. A line without a
    state, a step or an integer exit is a missing figure of its own."""
    path = os.path.join(run_dir, "stamps", "failed_states.jsonl")
    out = {}
    if not os.path.exists(path):
        return out
    for line in load_jsonl(path, problems, "stamps/failed_states.jsonl"):
        code = line.get("exit")
        if not line.get("state") or not line.get("step") or isinstance(code, bool) or not isinstance(code, int):
            problems.add("missing", f"stamps/failed_states.jsonl line without a state, step or integer exit: "
                                    f"{json.dumps(line, sort_keys=True)}")
            continue
        out.setdefault(line["state"], line)
    return out


def failed_reason(line):
    return f"state failed at {line['step']} (exit {line['exit']})"


def live_states(cfg, not_run, all_states):
    """{state: None if live else the reason it is not}."""
    out = {}
    for s in all_states:
        parent = DERIVED.get(s)
        if s in cfg["states"] or (parent and parent in cfg["states"]):
            reason = not_run.get(s) or (not_run.get(parent) if parent else None)
            out[s] = reason
        else:
            out[s] = not_run.get(s) or "not in run.toml states"
    return out


def tenant_of(state, label, cfg):
    if state.startswith("many_parts_"):
        return "mp" + state[len("many_parts_"):]
    return "hits" if label == "hits" else cfg["tenant"]


def labels_of(state, cfg):
    if state.startswith("many_parts_"):
        return ["spans"]
    return list(SIGNALS) + (["hits"] if cfg["hits"] else [])


def signal_of(label):
    return "logs" if label == "hits" else label


# --- answers ------------------------------------------------------------------


def truth_key_of(call, statements, cold_runs):
    """The truth key statements.toml gives this call, or None for an unknown
    statement. A saved-trace statement's cold (or in-flight) try i reads saved
    trace i and its warm repeat the last cold try's; a many-parts state reads
    its own corpus's answers (box/lib/inv.py plan)."""
    sid = call.get("statement") or ""
    if sid.startswith("control_"):
        return "hits.control"
    st = statements.get(sid)
    if st is None:
        return None
    state = call.get("state") or ""
    sub = state if state.startswith("many_parts_") else st.get("truth_sub")
    run = 0
    if st.get("per_run") == "saved_trace":
        run = (cold_runs or 1) - 1 if call.get("temp", "cold") == "warm" else call.get("try", 0)
    key = (st.get("truth_key_in") or {}).get(state, st["truth_key"])
    return (f"{sub}." if sub else "") + key.replace("{run}", str(run))


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


def answer_of(c):
    if "values" in c:
        return ("values", sorted(str(v).lower() for v in c["values"]))
    return ("sha256", c.get("values_sha256"))


def same_answer(a, b):
    return a.get("rows") == b.get("rows") and answer_of(a) == answer_of(b)


# --- per-call validation and metrics ------------------------------------------


def validate_call(c, problems):
    """Mark an ok call that lacks a field it must carry; True when valid."""
    at = where(c)
    lacking = [k for k in ("wall_s", "server_cpu_s", "server_hwm_bytes", "rows", "stats") if k not in c]
    stats = c.get("stats")
    if isinstance(stats, dict):
        phases = stats.get("phases")
        if not isinstance(phases, list):
            lacking.append("stats.phases")
        else:
            by = {p.get("phase"): p for p in phases if isinstance(p, dict)}
            for p in PHASES:
                if p not in by:
                    lacking.append(f"stats.phases.{p}")
                    continue
                lacking += [f"stats.phases.{p}.{f}" for f in PHASE_FIELDS if f not in by[p]]
        for member, field in (("pruning", "blocksScanned"), ("pruning", "segments"), ("io", "dependencyDepth")):
            if not isinstance(stats.get(member), dict) or field not in stats[member]:
                lacking.append(f"stats.{member}.{field}")
        if not isinstance(stats.get("signals"), list):
            lacking.append("stats.signals")
    elif "stats" in c:
        lacking.append("stats")
    for name in lacking:
        problems.add("missing", f"{name} in {at}")
    if lacking:
        c["_invalid"] = lacking[0]
    return not lacking


def phase_map(stats):
    return {p["phase"]: p for p in stats["phases"]}


def call_metrics(c):
    """Every metric of one validated ok call."""
    stats = c["stats"]
    ph = phase_map(stats)
    v = {}
    for p in PHASES:
        v[f"{p.lower()}_gets"] = ph[p]["s3GetRequests"]
        v[f"{p.lower()}_wire_bytes"] = ph[p]["s3GetWireBytes"]
    v["total_gets"] = sum(ph[p]["s3GetRequests"] for p in PHASES)
    v["data_gets"] = ph["probe"]["s3GetRequests"] + ph["scan"]["s3GetRequests"]
    v["list_requests"] = sum(ph[p]["s3ListRequests"] for p in PHASES)
    v["wire_bytes"] = sum(ph[p]["s3GetWireBytes"] for p in PHASES)
    v["decompressed_bytes"] = sum(ph[p]["decompressedOutputBytes"] for p in PHASES)
    v["segments_opened"] = sum(ph[p]["segmentsOpened"] for p in PHASES)
    v["cache_served_bytes"] = sum(ph[p]["cacheServedBytes"] for p in PHASES)
    v["blocks_scanned"] = stats["pruning"]["blocksScanned"]
    v["segments"] = stats["pruning"]["segments"]
    v["dependency_depth"] = stats["io"]["dependencyDepth"]
    v["signals_entries"] = len(stats["signals"])
    v["rows"] = c["rows"]
    return v


def allowlisted(c, name):
    """(value, None) or (None, unsupported reason) for an allowlisted metric."""
    stats = c["stats"]
    if name == "skip_idx_dropped":
        if "skipIdxDropped" in stats["pruning"]:
            return stats["pruning"]["skipIdxDropped"], None
        return None, "stats carry no pruning.skipIdxDropped on this build"
    field = KEY_INDEX_METRICS[name]
    ki = stats.get("keyIndex")
    if isinstance(ki, dict) and field in ki:
        return ki[field], None
    return None, f"stats carry no keyIndex.{field} on this build"


# --- the run's calls ----------------------------------------------------------


class Calls:
    def __init__(self, calls, problems, statements=None):
        self.groups = {}  # (statement, state, temp) -> {try: [lines]}
        self.wrong = []
        self.errors = []
        self.unjudged = 0
        statements = statements or {}
        for c in calls:
            key = (c.get("statement"), c.get("state"), c.get("temp", "cold"))
            self.groups.setdefault(key, {}).setdefault(c.get("try", 0), []).append(c)
        for key, tries in sorted(self.groups.items()):
            for t, lines in sorted(tries.items()):
                if len(lines) > 1:
                    problems.add("duplicate", f"call {key[0]}/{key[1]}/{key[2]} try {t}")
                for c in lines:
                    outcome = c.get("outcome")
                    # A refusal the server returned is refused-by-guard only on
                    # a statement planned as a cap probe, refused on any other.
                    if outcome in (UNSUPPORTED, GUARD) and "http_status" in c:
                        probe = (statements.get(c.get("statement") or "") or {}).get("expect_refusal_on_main")
                        outcome = c["outcome"] = GUARD if probe else "refused"
                    if outcome == "ok":
                        validate_call(c, problems)
                    elif outcome == "refused":
                        c["_judged"] = "refused"
                        problems.add("refused", f"{where(c)}: HTTP {c.get('http_status')} "
                                                f"{c.get('reason') or c.get('error') or 'no text'}")
                    elif outcome in (UNSUPPORTED, GUARD, "not-run", "server_died"):
                        if not c.get("reason"):
                            problems.add("missing", f"reason for {outcome} {where(c)}")
                        if outcome == "server_died":
                            # Whatever figure reads this group, and whether or
                            # not its state is live: a server that died under a
                            # call is a problem of the run, so the verdict is
                            # fail even when no band figure reads the group.
                            problems.add("server_died", f"{where(c)}: "
                                                        f"{c.get('reason') or 'server died, no reason recorded'}")
                        if outcome == GUARD and "wall_s" not in c:
                            problems.add("missing", f"wall_s in {where(c)}")
                    else:
                        c["_judged"] = "error"
                        self.errors.append(f"{where(c)}: outcome {outcome!r} HTTP {c.get('http_status')}")

    def tries(self, statement, state, temp):
        return self.groups.get((statement, state, temp), {})

    def judge_all(self, answers, statements, cfg, problems):
        """Judge every ok call: against truth, or against its unfolded twin."""
        for key, tries in sorted(self.groups.items()):
            statement, state, temp = key
            st = statements.get(statement or "")
            for t, lines in sorted(tries.items()):
                for c in lines:
                    if c.get("outcome") != "ok":
                        continue
                    tk = truth_key_of(c, statements, cfg["cold_runs"])
                    unfolded_oracle = st is not None and state not in (st.get("truth_key_in") or {}) and (
                        state in ORACLE_STATES or (st.get("oracle") == "unfolded" and state != "unfolded"))
                    if st is not None and st.get("oracle") == "unfolded" and state == "unfolded":
                        if c.get("truth_key") != tk:
                            verdict, detail = "wrong_answer", (f"call names truth key {c.get('truth_key')!r}, "
                                                               f"statements.toml gives {tk!r}")
                        else:
                            verdict, detail = "reference", ""
                    elif unfolded_oracle:
                        verdict, detail = self.judge_against_unfolded(c, tk, problems)
                    else:
                        verdict, detail = judge(c, answers, tk)
                    c["_judged"] = verdict
                    if verdict == "wrong_answer":
                        self.wrong.append(f"{where(c)}: {detail}")
                    elif verdict == "unjudged":
                        self.unjudged += 1

    def judge_against_unfolded(self, c, tk, problems):
        if c.get("truth_key") != tk:
            return "wrong_answer", f"call names truth key {c.get('truth_key')!r}, statements.toml gives {tk!r}"
        temp = "warm" if c.get("temp") == "warm" else "cold"
        twin = self.tries(c["statement"], "unfolded", temp).get(c.get("try", 0), [])
        if len(twin) != 1 or twin[0].get("outcome") != "ok":
            problems.add("missing", f"oracle call {c['statement']}/unfolded/{temp} try {c.get('try', 0)} "
                                    f"for {where(c)}")
            return "missing", "no unfolded call to compare against"
        if not same_answer(c, twin[0]):
            return "wrong_answer", (f"rows {c.get('rows')} differ from the unfolded answer's "
                                    f"{twin[0].get('rows')} or its row set")
        return "correct", ""


def static_group(tries):
    """A plan line that was never sent: one try 0, not-run, or unsupported
    with no HTTP status."""
    if list(tries) != [0] or len(tries[0]) != 1:
        return False
    c = tries[0][0]
    return c.get("outcome") == "not-run" or (c.get("outcome") == UNSUPPORTED and "http_status" not in c)


def check_tries(calls, cfg, problems):
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        if static_group(tries):
            continue
        want = cfg["warm_runs"] if temp == "warm" else cfg["cold_runs"]
        absent = [t for t in range(want) if t not in tries]
        if absent:
            problems.add("missing", f"{temp} tries {absent} of {statement} in {state}")
        extra = sorted(t for t in tries if t >= want or t < 0)
        if extra:
            problems.add("duplicate", f"{temp} tries {extra} of {statement} in {state} beyond the {want} "
                                      f"the run declares")


def expected_temps(st, state, cfg):
    if state in INFLIGHT_STATES:
        return ["inflight"]
    temps = ["cold"]
    if not st.get("cold_only", True) and cfg["warm_runs"]:
        temps.append("warm")
    return temps


def check_coverage(calls, statements, cfg, live, problems):
    """Every (statement, live query state) statements.toml names has calls.
    Returns {state: "run" | "missing"} for the query states that are live."""
    seen = {}
    for (statement, state, _temp) in calls.groups:
        seen.setdefault(state, set()).add(statement)
    out = {}
    for state, reason in sorted(live.items()):
        if reason is not None or state not in {s for st in statements.values() for s in st.get("states", [])}:
            continue
        if not seen.get(state):
            problems.add("missing", f"state {state}: no calls in calls.jsonl")
            out[state] = "missing"
            continue
        out[state] = "run"
        for sid, st in sorted(statements.items()):
            if state not in st.get("states", []):
                continue
            if st.get("tenant") == "hits" and not cfg["hits"]:
                continue
            if st.get("control"):
                if not any(s.startswith("control_") for s in seen[state]):
                    problems.add("missing", f"calls of control_* in {state}")
                continue
            cold = calls.tries(sid, state, "inflight" if state in INFLIGHT_STATES else "cold")
            if not cold:
                problems.add("missing", f"calls of {sid} in {state}")
                continue
            if static_group(cold):
                continue
            for temp in expected_temps(st, state, cfg)[1:]:
                if not calls.tries(sid, state, temp):
                    problems.add("missing", f"{temp} calls of {sid} in {state}")
    return out


def group_lines(calls, cfg, statement, state, temp):
    """(lines, None) for a complete group, or (None, (status, reason))."""
    tries = calls.tries(statement, state, temp)
    if not tries:
        return None, ("missing", f"no {temp} call of {statement} in {state}")
    if any(len(v) > 1 for v in tries.values()):
        return None, ("duplicate", f"two calls for one {temp} try of {statement} in {state}")
    if static_group(tries):
        c = tries[0][0]
        return [c], None
    want = cfg["warm_runs"] if temp == "warm" else cfg["cold_runs"]
    absent = [t for t in range(want) if t not in tries]
    if absent:
        return None, ("missing", f"{temp} tries {absent} of {statement} in {state}")
    extra = sorted(t for t in tries if t >= want)
    if extra:
        return None, ("duplicate", f"{temp} tries {extra} beyond the {want} the run declares")
    return [tries[t][0] for t in range(want)], None


def key_index_nonzero(c):
    """True when an ok call's stats carry a nonzero keyIndex figure."""
    ki = (c.get("stats") or {}).get("keyIndex")
    return isinstance(ki, dict) and bool(ki.get("keysProbed") or ki.get("partsCovered"))


def control_1001(calls, cfg, statement, state, temp):
    """Row 11's 1,001-key control of a 1,000-key IN statement.

    1 when every 1,000-key call probed exactly 1,000 keys with a nonzero
    partsCovered, and every 1,001-key call was either refused or served by a
    visible scan (probe or scan GETs, or blocks scanned) with no keyIndex
    figure above zero. 0 when a 1,001-key call carries a nonzero keyIndex
    figure, or was served with neither a keyIndex figure nor a scan, or a
    1,000-key call probed another count or covered no part. The 1,001-key
    arm is judged first; only a 1,000-key list that was itself refused, or
    that carries no tier-2 figures, makes the figure refused-by-guard or
    unsupported."""
    over = statement.replace("_1000", "_1001")
    lines, bad = group_lines(calls, cfg, statement, state, temp)
    if bad:
        return bad[0], None, bad[1]
    lines_over, bad = group_lines(calls, cfg, over, state, temp)
    if bad:
        return bad[0], None, bad[1]
    for c in lines_over:
        outcome = c.get("outcome")
        if outcome in (GUARD, UNSUPPORTED, "refused"):
            continue
        if outcome != "ok" or c.get("_invalid"):
            return "missing", None, f"{where(c)} neither served nor refused"
        if key_index_nonzero(c):
            ki = c["stats"]["keyIndex"]
            return "value", 0, (f"{where(c)} was index-served: keysProbed {ki.get('keysProbed')} "
                                f"partsCovered {ki.get('partsCovered')}")
        m = call_metrics(c)
        if m["probe_gets"] == 0 and m["scan_gets"] == 0 and m["blocks_scanned"] == 0:
            return "value", 0, (f"{where(c)} served without a visible path: no keyIndex figure, "
                                f"no probe or scan GET, no block scanned")
    guarded = [c for c in lines if c.get("outcome") == GUARD]
    if guarded:
        return GUARD, None, ("refused by the SQL complexity guard: "
                             + (guarded[0].get("reason") or "no reason given"))
    refused = [c for c in lines if c.get("outcome") == UNSUPPORTED]
    if refused:
        return UNSUPPORTED, None, f"the 1,000-key list is unsupported: {refused[0].get('reason') or 'no reason given'}"
    if any(c.get("outcome") != "ok" or c.get("_invalid") for c in lines):
        return "missing", None, f"a {temp} call of {statement} in {state} failed"
    for c in lines:
        ki = (c.get("stats") or {}).get("keyIndex")
        if not isinstance(ki, dict) or "keysProbed" not in ki or "partsCovered" not in ki:
            return UNSUPPORTED, None, f"{where(c)} carries no keyIndex.keysProbed and partsCovered on this build"
        if ki["keysProbed"] != 1000:
            return "value", 0, f"{where(c)} probed {ki['keysProbed']} keys, not 1000"
        if not ki["partsCovered"]:
            return "value", 0, f"{where(c)} probed 1000 keys with partsCovered {ki['partsCovered']}"
    return "value", 1, ""


def statement_figure(calls, cfg, statement, state, metric, candidates=None):
    """(status, value, reason) of one statement figure. candidates(c) is
    (count, None) or (None, reason): the objects the state's ranges stamp
    says overlap call c's window.

    metric is "<temp>.<agg>.<name>", "<temp>.rows_exact",
    "<temp>.rows_match_state.<other>", "<temp>.wall_p50_s" or
    "<temp>.control_1001".
    """
    temp, _, rest = metric.partition(".")
    if rest == "control_1001":
        return control_1001(calls, cfg, statement, state, temp)
    lines, bad = group_lines(calls, cfg, statement, state, temp)
    if bad:
        return bad[0], None, bad[1]
    outcomes = {c.get("outcome") for c in lines}
    if "refused" in outcomes:
        c = next(c for c in lines if c.get("outcome") == "refused")
        return "refused", None, c.get("reason") or "refused"
    if "not-run" in outcomes:
        # Some or all tries were not sent (the run deadline, an in-flight
        # window too short); a partial group has no figure either.
        c = next(c for c in lines if c.get("outcome") == "not-run")
        n = sum(1 for c in lines if c.get("outcome") == "not-run")
        return "not-run", None, (c.get("reason") or "no reason given") + (
            f" ({n} of {len(lines)} tries)" if n != len(lines) else "")
    if outcomes in ({UNSUPPORTED}, {GUARD}):
        return outcomes.pop(), None, lines[0].get("reason") or "no reason given"
    if "server_died" in outcomes:
        c = next(c for c in lines if c.get("outcome") == "server_died")
        return "missing", None, c.get("reason") or f"server died during {statement}/{state}"
    if outcomes != {"ok"}:
        return "missing", None, f"a {temp} call of {statement} in {state} failed"
    invalid = [c for c in lines if c.get("_invalid")]
    if invalid:
        return "missing", None, f"{invalid[0]['_invalid']} in {where(invalid[0])}"
    if rest == "rows_exact":
        if any(c.get("_judged") == "missing" for c in lines):
            return "missing", None, f"no oracle for {statement} in {state}"
        return "value", int(all(c.get("_judged") in ("correct", "reference") for c in lines)), ""
    if rest.startswith("rows_match_state."):
        other = rest.split(".", 1)[1]
        theirs, bad = group_lines(calls, cfg, statement, other, temp)
        if bad or len(theirs) != len(lines):
            return "missing", None, f"{temp} calls of {statement} in {other} to compare against"
        same = all(c.get("outcome") == "ok" and o.get("outcome") == "ok" and same_answer(c, o)
                   for c, o in zip(lines, theirs))
        return "value", int(same), ""
    if rest == "wall_p50_s":
        return "value", rnd(percentile([c["wall_s"] for c in lines], 0.5)), ""
    agg, _, name = rest.partition(".")
    head = name.split(".", 1)[0]
    if head in ALLOWLIST_BREAKDOWNS:
        return UNSUPPORTED, None, ALLOWLIST_BREAKDOWNS[head]
    vals = []
    for c in lines:
        m = call_metrics(c)
        if name in KEY_INDEX_METRICS or name == "skip_idx_dropped":
            v, why = allowlisted(c, name)
            if why:
                return UNSUPPORTED, None, why
        elif name == "probe_wire_per_object":
            # Candidate objects: the state's data objects whose event range
            # overlaps the call's window, from the ranges stamp, never a
            # figure the call reports about itself.
            probed, why = candidates(c) if candidates else (None, "no ranges stamp reader")
            if why:
                return "missing", None, why
            per_object = c["stats"].get("perObject")
            if isinstance(per_object, list) and len(per_object) != probed:
                return "missing", None, (f"{where(c)} perObject has {len(per_object)} entries, the ranges stamp "
                                         f"{probed} candidate objects")
            if probed == 0:
                return "missing", None, f"no candidate object for {where(c)} to divide the probe wire by"
            v = rnd(m["probe_wire_bytes"] / probed)
        elif name in m:
            v = m[name]
        else:
            return "missing", None, f"unknown metric {name}"
        vals.append(v)
    value = max(vals) if agg == "max" else min(vals)
    if name == "segments_opened" and value == 0:
        scanned = [c for c in lines if call_metrics(c)["scan_gets"] >= 1]
        if scanned:
            return "dead_counter", 0, (f"segmentsOpened 0 while {where(scanned[0])} issued "
                                       f"{call_metrics(scanned[0])['scan_gets']} scan GETs")
    return "value", value, ""


# --- stamps -------------------------------------------------------------------


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


class Stamps:
    def __init__(self, run_dir):
        self.dir = os.path.join(run_dir, "stamps")
        self.cache = {}

    def doc(self, state, label, kind):
        """stamps/STATE.LABEL.KIND.json, or None when absent or unreadable."""
        name = f"{state}.{label}.{kind}.json"
        if name not in self.cache:
            path = os.path.join(self.dir, name)
            try:
                with open(path) as f:
                    self.cache[name] = json.load(f)
            except (OSError, ValueError):
                self.cache[name] = None
        return self.cache[name]


def call_window_ns(st, truth, c):
    """(start_ns, end_ns) of a trace-anchored windowed call, as inv.py
    window_bounds computes it for the tenant's own corpus: the saved trace
    try c["try"] reads, plus or minus half the window."""
    half = {"1h": HOUR_NS // 2, "narrow": NARROW_NS // 2}.get(st.get("window"))
    saved = (truth or {}).get("saved_traces") or []
    if half is None or st.get("anchor", "trace") != "trace" or not saved or not isinstance(c.get("try"), int):
        return None
    centre = saved[c["try"] % len(saved)].get("start_ns")
    return (centre - half, centre + half) if isinstance(centre, int) else None


def window_candidates(stamps, statements, truth, c):
    """(count, None) of the data objects stamps/STATE.LABEL.ranges.json says
    overlap call c's window, or (None, reason)."""
    st = statements.get(c.get("statement")) or {}
    label = st.get("table", "spans")
    name = f"stamps/{c.get('state')}.{label}.ranges.json"
    doc = stamps.doc(c.get("state"), label, "ranges")
    if doc is None:
        return None, f"{name} absent: no per-object event ranges to count the window's candidate objects"
    window = call_window_ns(st, truth, c) if not str(c.get("state")).startswith("many_parts_") else None
    if window is None:
        return None, f"no trace-anchored window for {where(c)} in truth.json"
    objects = doc.get("objects")
    if not isinstance(objects, list) or not objects or any(
            not isinstance(o.get("min_ns"), int) or not isinstance(o.get("max_ns"), int) for o in objects):
        return None, f"{name} carries no min_ns and max_ns for every object"
    start, end = window
    return sum(1 for o in objects if o["min_ns"] < end and o["max_ns"] >= start), None


def required_stamps(run_dir, cfg, live, problems):
    stamps = os.path.join(run_dir, "stamps")
    docs = {}
    names = ["build.json", "host.json", "declare.json", "store.json", "corpus.json", "tenants.json"]
    if live.get("unfolded") is None:
        names.append("needle_blocks.json")
    for name in names:
        docs[name] = load_json(os.path.join(stamps, name), problems, f"stamps/{name}")
    build = docs.get("build.json")
    if isinstance(build, dict) and build.get("lane") == "local" and (
            isinstance(build.get("dirty_paths"), bool) or not isinstance(build.get("dirty_paths"), int)):
        problems.add("missing", "stamps/build.json dirty_paths: no count of the harness's uncommitted paths")

    def jsonl(name, keys):
        return index_lines(load_jsonl(os.path.join(stamps, name), problems, f"stamps/{name}"),
                           keys, f"stamps/{name}", problems)

    loads = jsonl("loads.jsonl", ("hour", "tenant", "signal"))
    folds = jsonl("folds.jsonl", ("transition", "tenant", "signal"))
    census = jsonl("census.jsonl", ("state",))
    restarts = jsonl("restarts.jsonl", ("statement", "state", "try"))
    transitions = jsonl("transitions.jsonl", ("state",))
    compaction = (jsonl("compaction.jsonl", ("signal",))
                  if live.get("compaction_in_flight") is None else {})
    erasure = jsonl("erasure.jsonl", ()) if live.get("erasure_in_flight") is None else {}
    on = [s for s, r in sorted(live.items()) if r is None]
    tenant = cfg["tenant"]

    want_loads = [("base", tenant, s) for s in SIGNALS]
    if "mixed" in on:
        want_loads += [("tail", tenant, s) for s in SIGNALS]
    if cfg["hits"]:
        want_loads.append(("base", "hits", "hits"))
    want_loads += [(s, tenant_of(s, "spans", cfg), "spans") for s in on if s.startswith("many_parts_")]
    for k in want_loads:
        line = loads.get(k, "absent")
        if line == "absent":
            problems.add("missing", f"stamps/loads.jsonl {'/'.join(k)}")
        elif line and line.get("exit") != 0:
            problems.add("failed", f"load {'/'.join(k)} exit {line.get('exit')}")
        elif line and not (line.get("report") or {}).get("objects_written"):
            problems.add("wrong", f"load {'/'.join(k)} report shows no objects_written")

    labels = labels_of("unfolded", cfg)
    want_folds = []
    # fold_advance's fold stamps are wanted when fold_advance itself is live:
    # live_states resolves it through DERIVED, so that is "mixed live and
    # fold_advance neither failed nor not-run". Gating them on mixed would
    # demand stamps a failed or skipped fold never wrote.
    for transition, when in (("folded_no_leaves", "folded_no_leaves"), ("folded_leaves", "folded_leaves"),
                             ("leaf_rebuild", "folded_leaves"), ("fold_advance", "fold_advance")):
        if when in on:
            want_folds += [(transition, tenant_of(transition, lab, cfg), lab) for lab in labels]
    for s in on:
        if s.startswith("many_parts_"):
            t = tenant_of(s, "spans", cfg)
            want_folds += [(s, t, "spans"), (f"{s}_leaf_rebuild", t, "spans")]
    for k in want_folds:
        line = folds.get(k, "absent")
        if line == "absent":
            problems.add("missing", f"stamps/folds.jsonl {'/'.join(k)}")
        elif line and line.get("exit") != 0:
            problems.add("failed", f"fold {'/'.join(k)} exit {line.get('exit')}")

    if live.get("compaction_in_flight") is None:
        for sig in SIGNALS:
            line = compaction.get((sig,), "absent")
            if line == "absent":
                problems.add("missing", f"stamps/compaction.jsonl {sig}")
            elif line and line.get("exit") != 0:
                problems.add("failed", f"compaction {sig} exit {line.get('exit')}")
            elif line and not ((line.get("report") or {}).get("fields") or {}).get("compacted"):
                problems.add("wrong", f"compaction {sig} compacted nothing, so no statement ran against a "
                                      f"compaction in flight")
    if live.get("erasure_in_flight") is None and not erasure:
        problems.add("missing", "stamps/erasure.jsonl")

    st = Stamps(run_dir)
    for s in on:
        if s != "compaction_in_flight":
            if (s,) not in census:
                problems.add("missing", f"stamps/census.jsonl {s}")
            for lab in labels_of(s, cfg):
                if st.doc(s, lab, "catalog") is None:
                    problems.add("missing", f"covering_parts.{s}.{lab}")
            if not os.path.exists(os.path.join(stamps, f"{s}.server.json")):
                problems.add("missing", f"stamps/{s}.server.json")
        if (s,) not in transitions:
            problems.add("missing", f"stamps/transitions.jsonl {s}")
    return docs, loads, folds, census, restarts, st, compaction


def check_loads(loads, truth, cfg, problems):
    rows = (truth or {}).get("rows_written", {})
    for sig in SIGNALS:
        line = loads.get(("base", cfg["tenant"], sig))
        if not line or sig not in rows:
            continue
        report = line.get("report") or {}
        # This build's load report counts objects, not rows; the row count is
        # then proven by the statements judged against truth.json.
        if "rows_written" in report and report["rows_written"] != rows[sig]:
            problems.add("wrong", f"load base/{sig} wrote {report['rows_written']} rows, "
                                  f"truth.json says {rows[sig]}")


def check_needle_blocks(doc, problems):
    """Per needle, the blocks gen_corpus planted it in (truth needle_blocks)
    beside the storage blocks its column is present in (inv.py
    needle-blocks). The rare needle is one row, so storage holds it in
    exactly the planted count whatever order it keeps rows in; storage sorts
    rows by (stream_ref, ts) within a shard and hour, so the sparse and common
    needles' counts move with that order and are banded within
    NEEDLE_BLOCK_BAND of planted, and the tag column (every needle) is
    reported only. Returns the comparison for the results file."""
    if not doc:
        return None
    planted = doc.get("truth_needle_blocks") or {}
    partial = doc.get("objects_inspected") != doc.get("objects_total")
    out = {"block_count": doc.get("block_count"), "record_count": doc.get("record_count"),
           "objects_inspected": doc.get("objects_inspected"), "objects_total": doc.get("objects_total"),
           "needles": {}, "figures": [], "incomplete": []}
    for value, column, band in NEEDLE_GEOMETRY:
        want = planted.get(value)
        got = doc.get(f"{column}_present_blocks")
        entry = {"column": column, "planted": want, "storage": got, "band": band}
        if isinstance(want, bool) or not isinstance(want, int):
            entry["status"] = "missing"
            problems.add("missing", f"needle_blocks {value}: truth.json needle_blocks carries no {value}")
        elif isinstance(got, bool) or not isinstance(got, int):
            entry["status"] = "missing"
            problems.add("missing", f"needle_blocks {value}: stamps/needle_blocks.json {column}_present_blocks "
                                    f"absent or null")
        elif band == "reported":
            entry["status"] = "reported"
        elif partial:
            # A partial inspection (needle_blocks_max_objects) can miss an
            # object holding a needle row: the band cannot be judged.
            entry["status"] = "incomplete"
            entry["reason"] = (f"needle_blocks {value}: {column} counted over {doc.get('objects_inspected')} of "
                               f"{doc.get('objects_total')} logs objects (run.toml needle_blocks_max_objects), "
                               f"{got} storage blocks against {want} planted, not judged")
            out["incomplete"].append(entry["reason"])
        elif band == "exact":
            entry["status"] = "match" if got == want else "wrong"
            if got != want:
                problems.add("wrong", f"{column} in {got} storage blocks, truth.json says {want}")
        elif abs(got - want) <= NEEDLE_BLOCK_BAND * want:
            entry["status"] = "within"
        else:
            entry["status"] = "miss"
            problems.add("miss", f"needle_blocks {value}: {column} in {got} storage blocks, {want} planted, "
                                 f"outside {NEEDLE_BLOCK_BAND:.0%}")
        out["needles"][value] = entry
        out["figures"].append({"id": f"needle_blocks.{value}.planted", "value": want})
        out["figures"].append({"id": f"needle_blocks.{value}.storage", "value": got, "status": entry["status"]})
    return out


def check_inflight(calls, statements, compaction, problems, live):
    """Every in-flight try must overlap the compaction of the signal its
    statement reads: [started_unix_s, started_unix_s + wall_s] against the
    compaction's measured [started_unix_s, ended_unix_s]. A compaction that
    ended before the statement's first try, or ran shorter than one call
    without covering every try, could not be overlapped: the group is
    recorded not-run with that duration. Any other partial overlap is a
    not_in_flight problem."""
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        if state not in INFLIGHT_STATES or temp != "inflight" or live.get(state) is not None:
            continue
        lines = [c for v in tries.values() for c in v if c.get("outcome") == "ok"]
        if not lines:
            continue
        sig = (statements.get(statement) or {}).get("table", "spans")
        measure = ((compaction.get((sig,)) or {}).get("measure") or {})
        started, ended = measure.get("started_unix_s"), measure.get("ended_unix_s")
        if not isinstance(started, (int, float)) or not isinstance(ended, (int, float)):
            problems.add("missing", f"stamps/compaction.jsonl {sig} measure started_unix_s/ended_unix_s "
                                    f"for {statement}/{state}")
            continue
        if any(not isinstance(c.get("started_unix_s"), (int, float)) or not isinstance(c.get("wall_s"), (int, float))
               for c in lines):
            problems.add("missing", f"started_unix_s or wall_s in an in-flight call of {statement}")
            continue
        overlapped = sum(1 for c in lines if c["started_unix_s"] < ended and c["started_unix_s"] + c["wall_s"] > started)
        if overlapped == len(lines):
            continue
        first = min(c["started_unix_s"] for c in lines)
        ran_ms = round((ended - started) * 1000)
        shortest = min(c["wall_s"] for c in lines)
        if ended <= first:
            reason = f"compaction finished in {ran_ms} ms before the first call"
        elif ended - started < shortest:
            reason = f"compaction ran {ran_ms} ms, shorter than one call ({round(shortest * 1000)} ms)"
        else:
            problems.add("not_in_flight", f"{statement}/{state}: {overlapped} of {len(lines)} inflight tries "
                                          f"overlapped the {sig} compaction")
            continue
        for c in lines:
            c["outcome"] = "not-run"
            c["reason"] = reason


def check_restarts(calls, restarts, problems):
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        if temp != "cold":
            continue
        for t, lines in sorted(tries.items()):
            sent = any(c.get("outcome") in ("ok", "refused", GUARD) for c in lines)
            if sent and (statement, state, t) not in restarts:
                problems.add("missing", f"stamps/restarts.jsonl {statement}/{state}/{t}")


# A stamp field the inspector did not print is missing, never 0: each helper
# below returns None when any input it sums is absent.


def int_or_none(values):
    values = list(values)
    return sum(values) if all(isinstance(v, int) for v in values) else None


def catalog_entries(doc):
    if "entries" in doc:
        return doc["entries"] if isinstance(doc["entries"], int) else None
    return int_or_none(p.get("entry_count") for p in doc.get("parts", []))


def leaf_bytes(doc, field=None):
    return int_or_none(leaf.get("bytes") for p in doc.get("parts", []) for leaf in p.get("leaves", [])
                       if field is None or leaf.get("field") == field)


def part_count(doc):
    v = doc.get("part_count")
    return v if isinstance(v, int) else None


def stamp_figure(stamps, census, cfg, state, metric):
    """(status, value, reason) of stamp.*, leaf_share.* and census.* figures."""
    kind, _, rest = metric.partition(".")
    if kind == "census":
        tenant, signal, field = rest.split(".")
        line = census.get((state,), "absent")
        if line == "absent":
            return "missing", None, f"no census of {state}"
        if line is None:
            return "duplicate", None, f"two censuses of {state}"
        data = ((line.get("census") or {}).get("tenants") or {}).get(tenant, {}).get("data", {}).get(signal)
        if not data or field not in data:
            return "missing", None, f"census of {state} has no {tenant}/{signal} {field}"
        return "value", data[field], ""
    if kind == "leaf_share":
        label, field = rest.split(".")
        doc = stamps.doc(state, label, "catalog")
        if doc is None:
            return "missing", None, f"covering_parts.{state}.{label}"
        line = census.get((state,))
        tenant = tenant_of(state, label, cfg)
        data = (((line or {}).get("census") or {}).get("tenants") or {}).get(tenant, {}).get(
            "data", {}).get(signal_of(label), {})
        if not data.get("bytes"):
            return "missing", None, f"census of {state} has no {tenant}/{signal_of(label)} bytes"
        if not any(leaf.get("field") == field for p in doc.get("parts", []) for leaf in p.get("leaves", [])):
            return "missing", None, f"stamps/{state}.{label}.catalog.json names no {field} leaf"
        lb = leaf_bytes(doc, field)
        if lb is None:
            return "missing", None, f"a {field} leaf in stamps/{state}.{label}.catalog.json has no size"
        return "value", rnd(100.0 * lb / data["bytes"]), ""
    what, _, label = rest.partition(".")
    if what == "footer_len":
        doc = stamps.doc(state, label, "object")
        if doc is None:
            return "missing", None, f"no stamps/{state}.{label}.object.json"
        if not isinstance(doc.get("footer_len"), int):
            return "missing", None, f"stamps/{state}.{label}.object.json carries no footer length"
        return "value", doc["footer_len"], ""
    if what == "buckets":
        doc = stamps.doc(state, label, "kidx")
        if doc is None or not doc.get("buckets"):
            return "missing", None, f"no leaf bucket count in stamps/{state}.{label}.kidx.json"
        return "value", int(str(doc["buckets"]).split()[0]), ""
    doc = stamps.doc(state, label, "catalog")
    if doc is None:
        return "missing", None, f"covering_parts.{state}.{label}"
    name = f"stamps/{state}.{label}.catalog.json"
    if what == "covering_parts":
        v = part_count(doc)
        return ("value", v, "") if v is not None else ("missing", None, f"{name} carries no part_count")
    if what == "entries":
        v = catalog_entries(doc)
        return ("value", v, "") if v is not None else ("missing", None, f"{name}: a part has no entry_count")
    if what == "leaves":
        return "value", sum(len(p.get("leaves", [])) for p in doc.get("parts", [])), ""
    if what == "part_entries":
        counts = [p.get("entry_count") for p in doc.get("parts", [])]
        if not counts or not all(isinstance(c, int) for c in counts):
            return "missing", None, f"{name}: no parts, or a part has no entry_count"
        return "value", max(counts), ""
    if what == "leaf_bytes":
        v = leaf_bytes(doc)
        return ("value", v, "") if v is not None else ("missing", None, f"{name}: a leaf has no size")
    return "missing", None, f"unknown stamp figure {what}"


def fold_field(report, field):
    for name in FOLD_FIELDS.get(field, (field,)):
        if name in report:
            return report[name]
    return None


def fold_figure(folds, transition, tenant, label, field):
    """(status, value, reason) of fold.<tenant>.<label>.<field>."""
    line = folds.get((transition, tenant, label), "absent")
    if line == "absent":
        return "missing", None, f"no fold stamp for {transition}/{tenant}/{label}"
    if line is None:
        return "duplicate", None, f"two fold stamps for {transition}/{tenant}/{label}"
    if field == "peak_rss_bytes":
        v = (line.get("measure") or {}).get("peak_rss_bytes")
        return ("value", v, "") if v is not None else ("missing", None, "fold stamp has no measure")
    v = fold_field(line.get("report") or {}, field)
    if v is None:
        if field in FOLD_ALLOWLIST:
            return UNSUPPORTED, None, f"fold report carries no {field} on this build"
        return "missing", None, f"fold report {transition}/{tenant}/{label} has no {field}"
    return "value", v, ""


# --- rows ---------------------------------------------------------------------


def row_not_run(row, cfg, live):
    if row.get("requires_tenant") == "hits" and not cfg["hits"]:
        return "the hits tenant is not loaded (run.toml hits = false)"
    lacking = [s for s in row.get("requires_states", []) if live.get(s, "unknown state") is not None]
    if lacking:
        return f"states {lacking} not run: " + "; ".join(f"{s}: {live.get(s, 'unknown state')}" for s in lacking)
    scales = [s for s in row.get("requires_scales", []) if s not in cfg["scales"]]
    if scales:
        return f"scales {scales} not in run.toml scales"
    if row.get("requires_baseline") and not cfg["baseline"]:
        return "no baseline build (run.toml baseline = false)"
    return None


def unrequested(cfg, live, tenant=None, scales=(), baseline=False, states=()):
    """The run.toml settings that leave a row or figure out: hits, scales,
    baseline, or a state run.toml never lists. Empty when it was asked for."""
    why = []
    if tenant == "hits" and not cfg["hits"]:
        why.append("hits = false")
    scales = [s for s in scales if s not in cfg["scales"]]
    if scales:
        why.append(f"scales lacks {scales}")
    if baseline and not cfg["baseline"]:
        why.append("baseline = false")
    states = [s for s in dict.fromkeys(states) if live.get(s) == "not in run.toml states"]
    if states:
        why.append(f"states lacks {states}")
    return "run.toml " + ", ".join(why) if why else None


def row_unrequested(row, cfg, live, fig=None):
    states = list(row.get("requires_states", []))
    if fig is not None:
        states += list(fig.get("requires_states", [])) + ([fig["state"]] if fig["state"] != "all" else [])
    return unrequested(cfg, live, row.get("requires_tenant"), row.get("requires_scales", []),
                       row.get("requires_baseline"), states)


def not_requested(bands, cfg, live, figures):
    """Gated rows run.toml never asked for, each with the setting that leaves
    it out, then the gated figures of other rows that run.toml never asked
    for (a figure naming a state the run does not list), by row and reason.
    A requested state that did not run is incomplete, not listed."""
    out = []
    for row in bands["row"]:
        why = row_unrequested(row, cfg, live)
        if why:
            out.append({"row": row["adr_row"], "key": row["key"], "reason": why})
    listed = {r["key"] for r in out}
    groups = {}
    for f in figures:
        key = f["id"].split(".", 1)[0]
        if f.get("gated") and f.get("not_requested") and key not in listed:
            groups.setdefault((f["row"], key, f["not_requested"]), []).append(f["id"].split(".", 1)[1])
    for (row, key, why), names in groups.items():
        out.append({"row": row, "key": key, "figures": names, "reason": why})
    return sorted(out, key=lambda r: r["row"])


def figure_not_run(fig, live):
    states = list(fig.get("requires_states", []))
    if fig["state"] != "all":
        states.append(fig["state"])
    lacking = [s for s in states if live.get(s, "unknown state") is not None]
    if lacking:
        return "; ".join(f"{s}: {live.get(s, 'unknown state')}" for s in lacking)
    return None


def evaluate_expr(expr, v, m):
    return bool(eval(expr, {"__builtins__": {"min": min, "max": max}}, {"v": v, "m": m}))  # noqa: S307 - bands.toml is repo-controlled


def derived_value(expr, figures_by_id):
    """(status, value, reason) of a derived figure's expression."""
    values = {k: e["value"] for k, e in figures_by_id.items() if e["status"] == "value"}
    try:
        v = eval(expr, {"__builtins__": {"min": min, "max": max}}, {"m": values})  # noqa: S307
    except KeyError as e:
        ref = figures_by_id.get(e.args[0])
        if ref is None:
            return "missing", None, f"refers to figure {e.args[0]} that bands.toml does not define before it"
        if ref["status"] in ("not-run", UNSUPPORTED, GUARD):
            return ref["status"], None, f"{e.args[0]} is {ref['status']}: {ref['reason']}"
        return "missing", None, f"{e.args[0]} has no value ({ref['status']})"
    return "value", rnd(v), ""


def truth_figure(truth, metric):
    """(status, value, reason) of truth.<path>: an integer truth.json holds."""
    v = truth
    for part in metric.split(".")[1:]:
        v = v.get(part) if isinstance(v, dict) else None
    if not isinstance(v, int) or isinstance(v, bool):
        return "missing", None, f"truth.json carries no integer {metric[len('truth.'):]}"
    return "value", v, ""


def run_rows(bands, cfg, live, calls, stamps, folds, census, matrix, cost_figs, truth, candidates=None):
    figures = []
    by_id = {}
    for row in bands["row"]:
        reason = row_not_run(row, cfg, live)
        for fig in row["figure"]:
            fid = f"{row['key']}.{fig['name']}"
            entry = {"id": fid, "row": row["adr_row"], "statement": fig["statement"], "state": fig["state"],
                     "metric": fig["metric"], "lane": fig["lane"]}
            metric = fig["metric"]
            why_not = reason or figure_not_run(fig, live)
            if why_not:
                status, value, why = "not-run", None, why_not
                entry["gated"] = True
                gap = row_unrequested(row, cfg, live, fig)
                if gap:
                    entry["not_requested"] = gap
            elif metric == "derived":
                status, value, why = derived_value(fig["expr"], by_id)
                refs = [by_id.get(r, {}) for r in re.findall(r"m\['([^']+)'\]", fig["expr"])]
                if status == "not-run" and any(r.get("gated") for r in refs):
                    entry["gated"] = True
                    gap = next((r["not_requested"] for r in refs if r.get("not_requested")), None)
                    if gap:
                        entry["not_requested"] = gap
            elif metric.startswith(("stamp.", "leaf_share.", "census.")):
                status, value, why = stamp_figure(stamps, census, cfg, fig["state"], metric)
            elif metric.startswith("fold."):
                _, tenant, label, field = metric.split(".", 3)
                status, value, why = fold_figure(folds, fig.get("transition", fig["state"]), tenant, label, field)
            elif metric.startswith("matrix."):
                status, value, why = matrix(metric, fig["state"])
            elif metric.startswith("cost."):
                status, value, why = "value", cost_figs[metric], ""
            elif metric.startswith("truth."):
                status, value, why = truth_figure(truth or {}, metric)
            elif metric.startswith(("scale.", "control.")):
                status, value, why = "missing", None, f"{metric} needs a run this harness lane does not perform"
            else:
                status, value, why = statement_figure(calls, cfg, fig["statement"], fig["state"], metric,
                                                      candidates)
            entry.update(status=status, value=value, reason=why)
            figures.append(entry)
            by_id[fid] = entry
    values = {f["id"]: f["value"] for f in figures if f["status"] == "value"}
    for row, f in ((r, f) for r in bands["row"] for f in r["figure"]):
        entry = by_id[f"{row['key']}.{f['name']}"]
        if entry["status"] != "value":
            entry["verdict"] = entry["status"]
            continue
        if f["lane"] == "timing" and cfg["lane"] != "s3":
            entry["verdict"] = "reported (timing is judged on the s3 lane only)"
            continue
        try:
            ok = evaluate_expr(f.get("target_if", "True"), entry["value"], values)
            miss = evaluate_expr(f.get("miss_if", "False"), entry["value"], values)
        except KeyError as e:
            ref = by_id.get(e.args[0])
            if ref is not None and ref["status"] in ("not-run", UNSUPPORTED, GUARD):
                entry["verdict"] = ref["status"]
                entry["reason"] = f"band refers to figure {e.args[0]}, {ref['status']}: {ref['reason']}"
            else:
                entry["verdict"] = "missing"
                entry["reason"] = f"band refers to figure {e.args[0]} that has no value"
            continue
        entry["verdict"] = "pass" if ok and not miss else "miss"
        if miss and f.get("miss_reason"):
            entry["reason"] = f["miss_reason"]
    return figures


# --- cost ---------------------------------------------------------------------


def price_basis(cfg):
    prices = cfg["prices"]
    return {k: {"value": prices.get(k), "source": prices.get(f"{k}_source")} for k in PRICE_KEYS}


def statement_cost(calls, cfg):
    """Dollars per cold call of every (statement, state) with valid ok calls.
    CPU is /proc utime+stime, read in 10 ms ticks, so a cheap call can read
    0; server_cpu_s_cold_sum is the sum over the cold tries, the per-statement
    figure that tick quantisation distorts least."""
    p = cfg["prices"]
    out = []
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        lines = [v[0] for v in tries.values() if len(v) == 1 and v[0].get("outcome") == "ok"
                 and not v[0].get("_invalid")]
        if temp != "cold" or not lines or len(lines) != len(tries):
            continue
        mets = [call_metrics(c) for c in lines]
        cpu = [c["server_cpu_s"] for c in lines]
        n = len(lines)
        g = sum(m["total_gets"] for m in mets) / n
        li = sum(m["list_requests"] for m in mets) / n
        s = sum(cpu) / n
        dollars = g * p["get_per_1000"] / 1000 + li * p["put_per_1000"] / 1000 + s * p["instance_per_hour"] / 3600
        out.append({
            "statement": statement, "state": state, "calls": n,
            "mean_gets": rnd(g), "mean_list_requests": rnd(li), "mean_server_cpu_s": rnd(s),
            "server_cpu_s_cold_sum": rnd(sum(cpu)),
            "server_cpu_s_p50": rnd(percentile(cpu, 0.5)),
            "server_hwm_bytes_max": max(c["server_hwm_bytes"] for c in lines),
            "dollars_per_call": rnd(dollars),
            "basis": "get_per_1000, put_per_1000 (LIST), instance_per_hour",
        })
    return out


def maintenance_cost(folds, stamps, cfg, live, problems):
    """Decision 8's amortised maintenance per signal and part-month: the leaf
    rebuild's section GETs (report `ranged_gets`) at the GET price and leaf
    PUTs (`attempts[].leaf_puts`) at the PUT price, over amortise_months,
    plus the leaf bytes the folded_leaves catalog stamp names at the storage
    price, divided by that stamp's part count."""
    p = cfg["prices"]
    months = cfg["cost"].get("amortise_months")
    out = []
    if live.get("folded_leaves") is not None:
        return out
    for label in labels_of("folded_leaves", cfg):
        tenant = tenant_of("folded_leaves", label, cfg)
        line = folds.get(("leaf_rebuild", tenant, label))
        doc = stamps.doc("folded_leaves", label, "catalog")
        entry = {"signal": label, "tenant": tenant, "basis":
                 "leaf_rebuild report ranged_gets x get_per_1000 + attempts[].leaf_puts x put_per_1000, "
                 "over amortise_months; folded_leaves catalog stamp leaf bytes x storage_gb_month; "
                 "per stamped part"}
        report = (line or {}).get("report") or {}
        attempts = report.get("attempts")
        puts = (int_or_none(a.get("leaf_puts") for a in attempts) if isinstance(attempts, list) and attempts
                else None)
        leaf = leaf_bytes(doc) if doc is not None else None
        parts = part_count(doc) if doc is not None else None
        if months is None:
            entry["status"] = "missing"
            out.append(entry)
            continue
        if not line or not isinstance(report.get("ranged_gets"), int) or puts is None or leaf is None or not parts:
            entry["status"] = "missing"
            problems.add("missing", f"maintenance inputs for {label} (leaf_rebuild report ranged_gets and "
                                    f"attempts[].leaf_puts, stamps/folded_leaves.{label}.catalog.json "
                                    f"part_count and leaf bytes)")
            out.append(entry)
            continue
        gets = report["ranged_gets"]
        build = gets * p["get_per_1000"] / 1000 + puts * p["put_per_1000"] / 1000
        storage = leaf / 1e9 * p["storage_gb_month"]
        total = build / months + storage
        entry.update(status="value", section_gets=gets, leaf_puts=puts, leaf_bytes=leaf, parts=parts,
                     amortise_months=months, build_dollars=rnd(build), storage_dollars_per_month=rnd(storage),
                     maintenance_per_part_month=rnd(total / parts))
        out.append(entry)
    return out


def storage_cost(census, cfg, problems):
    p = cfg["prices"]
    out = []
    for (state,), line in sorted(census.items()):
        if not line:
            continue
        b = (line.get("census") or {}).get("bytes")
        entry = {"state": state, "objects": (line.get("census") or {}).get("objects"), "bytes": b,
                 "basis": "storage_gb_month"}
        if isinstance(b, bool) or not isinstance(b, int):
            entry.update(status="missing", dollars_per_month=None)
            problems.add("missing", f"storage cost for {state}: census line carries no census.bytes")
        else:
            entry.update(status="value", dollars_per_month=rnd(b / 1e9 * p["storage_gb_month"]))
        out.append(entry)
    return out


def break_even(stmt_cost, maint, statements, cfg):
    """Per statement and signal: the lookups per part-month at which the
    saving of folded_leaves over unfolded pays the leaf's maintenance, and
    the folded arm's cost per lookup with that maintenance amortised over
    the assumed query frequency (lookups_per_part_month)."""
    freq = cfg["cost"].get("lookups_per_part_month")
    freq_src = cfg["cost"].get("lookups_per_part_month_source")
    by = {(s["statement"], s["state"]): s for s in stmt_cost}
    per_label = {m["signal"]: m for m in maint}
    out = []
    for (statement, state), s in sorted(by.items()):
        if state != "folded_leaves" or (statement, "unfolded") not in by:
            continue
        st = statements.get(statement, {})
        label = "hits" if st.get("tenant") == "hits" else st.get("table", "spans")
        m = per_label.get(label)
        saving = by[(statement, "unfolded")]["dollars_per_call"] - s["dollars_per_call"]
        entry = {"statement": statement, "signal": label,
                 "dollars_unfolded": by[(statement, "unfolded")]["dollars_per_call"],
                 "dollars_folded_leaves": s["dollars_per_call"], "saving_per_call": rnd(saving)}
        if m is None or m["status"] != "value":
            entry["break_even_lookups_per_part_month"] = "missing (no maintenance figure for the signal)"
        else:
            entry["maintenance_per_part_month"] = m["maintenance_per_part_month"]
            entry["break_even_lookups_per_part_month"] = (
                rnd(m["maintenance_per_part_month"] / saving) if saving > 0
                else "not applicable (folded cost per lookup is not below unfolded)")
            if isinstance(freq, (int, float)) and freq > 0:
                per = m["maintenance_per_part_month"] / freq
                entry["maintenance_per_lookup"] = rnd(per)
                entry["dollars_folded_leaves_with_maintenance"] = rnd(s["dollars_per_call"] + per)
                entry["maintenance_per_lookup_basis"] = (
                    f"maintenance_per_part_month / {freq} lookups per part-month (source: {freq_src})")
            else:
                entry["dollars_folded_leaves_with_maintenance"] = "missing (no lookups_per_part_month)"
        out.append(entry)
    return out


# --- reports ------------------------------------------------------------------


def statement_detail(calls):
    out = []
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        lines = [v[0] for _, v in sorted(tries.items())]
        outcomes = {c.get("outcome") for c in lines}
        if outcomes in ({UNSUPPORTED}, {GUARD}, {"not-run"}):
            out.append({"statement": statement, "state": state, "temp": temp, "status": outcomes.pop(),
                        "reason": lines[0].get("reason")})
            continue
        ok = [c for c in lines if c.get("outcome") == "ok" and not c.get("_invalid")]
        d = {"statement": statement, "state": state, "temp": temp, "calls": len(lines), "ok": len(ok),
             "correct": sum(1 for c in ok if c.get("_judged") in ("correct", "reference"))}
        if ok:
            walls = [c["wall_s"] for c in ok]
            cpu = [c["server_cpu_s"] for c in ok]
            d.update(wall_p50_s=rnd(percentile(walls, 0.5)), wall_p95_s=rnd(percentile(walls, 0.95)),
                     wall_p99_s=rnd(percentile(walls, 0.99)), server_cpu_s_p50=rnd(percentile(cpu, 0.5)),
                     server_hwm_bytes_max=max(c["server_hwm_bytes"] for c in ok))
            mets = [call_metrics(c) for c in ok]
            for name in ("total_gets", "data_gets", "resolve_gets", "probe_gets", "scan_gets", "keyindex_gets",
                         "list_requests", "wire_bytes", "decompressed_bytes", "segments_opened", "blocks_scanned",
                         "segments", "dependency_depth", "signals_entries", "cache_served_bytes"):
                d[f"max_{name}"] = max(m[name] for m in mets)
            if temp == "cold":
                d["warm_served"] = sum(1 for m in mets if m["cache_served_bytes"] > 0)
        out.append(d)
    return out


def warm_served_calls(calls):
    out = []
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        if temp != "cold":
            continue
        for t, lines in sorted(tries.items()):
            for c in lines:
                if c.get("outcome") == "ok" and not c.get("_invalid") and call_metrics(c)["cache_served_bytes"] > 0:
                    out.append({"statement": statement, "state": state, "try": t,
                                "cache_served_bytes": call_metrics(c)["cache_served_bytes"]})
    return out


def incomplete_reasons(cfg, live, calls, figures, needles=None):
    """Why a run with no problem still proved less than it was asked to: a
    state run.toml asks for that did not run, a planned call that was not
    sent in a state that did, or a figure not-run for any reason other than
    its row or figure lacking a state, tenant, scale or baseline the run
    never asked for (a gated figure is either not_requested or listed
    here), or a needle band a partial inspection could not judge."""
    out = []
    for s in cfg["states"]:
        if live.get(s) is not None:
            out.append(f"state {s} not run: {live[s]}")
    for (statement, state, temp), tries in sorted(calls.groups.items()):
        if live.get(state) is not None:
            continue
        lines = [c for v in tries.values() for c in v if c.get("outcome") == "not-run"]
        if lines:
            out.append(f"{len(lines)} {temp} call(s) of {statement} in {state} not run: "
                       f"{lines[0].get('reason') or 'no reason given'}")
    for f in figures:
        if f["status"] == "not-run" and not f.get("not_requested"):
            out.append(f"figure {f['id']} not run: {f['reason']}")
    out.extend((needles or {}).get("incomplete", []))
    return out


def summary_text(res):
    out = [f"selective investigations, lane {res['lane']}: {res['verdict']}"]
    if res["not_requested"]:
        out.append("not requested: " + "; ".join(
            f"row {r['row']}" + (f" figures {', '.join(r['figures'])}" if r.get("figures") else "")
            + f" ({r['reason']})" for r in res["not_requested"]))
    k = res["refusal_classes"]
    out.append(f"refusal classes: {GUARD} {k[GUARD]['figures']} figure(s), {k[GUARD]['calls']} call(s); "
               f"{UNSUPPORTED} {k[UNSUPPORTED]['figures']} figure(s), {k[UNSUPPORTED]['calls']} call(s)")
    out.append("")
    out.append("states:")
    for s in res["states"]:
        out.append(f"  {s['state']:24} {s['status']}" + (f"  ({s['reason']})" if s.get("reason") else ""))
    nb = res.get("needle_blocks")
    if nb:
        out.append("")
        out.append(f"needle blocks, planted / storage, over {nb['objects_inspected']} of {nb['objects_total']} "
                   f"logs object(s):")
        for value, e in nb["needles"].items():
            band = f"within {NEEDLE_BLOCK_BAND:.0%}" if e["band"] == "band" else e["band"]
            planted, storage = ("-" if n is None else str(n) for n in (e["planted"], e["storage"]))
            out.append(f"  {value:14} {e['column']:14} {planted:>5} / {storage:<5} "
                       f"{band:11} {e['status']}")
    out.append("")
    out.append(f"{'figure':44} {'status':12} {'value':>14}  verdict")
    for f in res["figures"]:
        v = "" if f["value"] is None else str(f["value"])
        out.append(f"{f['id']:44} {f['status'][:12]:12} {v:>14}  {f['verdict']}"
                   + (f"  ({f['reason']})" if f["reason"] else ""))
    out.append("")
    out.append("statement                       state                temp      p50_s     p95_s  gets  wire_B"
               "   cpu_s   $/call  warm_served")
    for s in res["statements"]:
        head = f"{s['statement'][:31]:31} {s['state'][:20]:20} {s['temp'][:8]:8}"
        if "status" in s:
            out.append(f"{head} {s['status']} ({s['reason']})")
            continue
        if not s["ok"]:
            out.append(f"{head} no valid ok call")
            continue
        out.append(f"{head} {s['wall_p50_s']:>7} {s['wall_p95_s']:>9} {s['max_total_gets']!s:>5} "
                   f"{s['max_wire_bytes']!s:>7} {s['server_cpu_s_p50']:>7} {s.get('dollars_per_call', '')!s:>8}"
                   f"  {s.get('warm_served', '')}")
    out.append("")
    out.append("prices:")
    c = res["cost"]
    for k, b in c["prices"].items():
        out.append(f"  {k} = {b['value']}  source: {b['source']}")
    out.append(f"query frequency assumed (once, for every break-even below): "
               f"{c['lookups_per_part_month_assumed']} lookups per part-month "
               f"(source: {c['lookups_per_part_month_source']}); leaf build amortised over "
               + (f"{c['amortise_months']} month(s)" if c["amortise_months"] is not None
                  else "missing (run.toml cost.amortise_months absent)"))
    for m in c["maintenance"]:
        if m["status"] != "value":
            out.append(f"  maintenance {m['signal']}: missing")
            continue
        out.append(f"  maintenance {m['signal']}: {m['section_gets']} section GET, {m['leaf_puts']} leaf PUT, "
                   f"{m['leaf_bytes']} leaf B over {m['parts']} part(s) = ${m['maintenance_per_part_month']}"
                   f"/part-month")
    for b in c["break_even"]:
        out.append(f"  {b['statement']} ({b['signal']}): unfolded ${b['dollars_unfolded']} folded "
                   f"${b['dollars_folded_leaves']} (with maintenance "
                   f"${b.get('dollars_folded_leaves_with_maintenance', 'missing')}) saving "
                   f"${b['saving_per_call']}/call; break-even "
                   f"{b['break_even_lookups_per_part_month']} lookups/part-month")
    if res["problems"]:
        out.append("")
        out.extend(res["problems"])
    if res["incomplete"]:
        out.append("")
        out.extend(f"incomplete {r}" for r in res["incomplete"])
    return "\n".join(out) + "\n"


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

    not_run_path = os.path.join(args.run, "stamps", "not_run.jsonl")
    not_run = {}
    if os.path.exists(not_run_path):
        for line in load_jsonl(not_run_path, problems, "stamps/not_run.jsonl"):
            not_run.setdefault(line.get("state"), line.get("reason") or "no reason given")
    failed = failed_states(args.run, problems)
    for state, line in failed.items():
        not_run[state] = failed_reason(line)
    query_states = sorted({s for st in statements.values() for s in st.get("states", [])})
    all_states = sorted(set(query_states) | set(DERIVED) | set(cfg["states"]))
    live = live_states(cfg, not_run, all_states)

    calls = Calls(calls_raw, problems, statements)
    docs, loads, folds, census, restarts, stamps, compaction = required_stamps(args.run, cfg, live, problems)
    check_inflight(calls, statements, compaction, problems, live)
    calls.judge_all(answers, statements, cfg, problems)
    check_tries(calls, cfg, problems)
    coverage = check_coverage(calls, statements, cfg, live, problems)
    needles = check_needle_blocks(docs.get("needle_blocks.json"), problems)
    check_loads(loads, truth, cfg, problems)
    check_restarts(calls, restarts, problems)
    for w in calls.wrong:
        problems.add("wrong_answer", w)
    for e in calls.errors:
        problems.add("error", e)

    def matrix(metric, state):
        if metric == "matrix.wrong_answers":
            return "value", len(calls.wrong) + len(calls.errors), ""
        if metric == "matrix.negative_controls":
            return "not-run", None, "mutant controls not implemented"
        groups = {k: [c for v in t.values() for c in v] for k, t in sorted(calls.groups.items()) if k[1] == state}
        unsent = {k: lines for k, lines in groups.items() if all(c.get("outcome") == "not-run" for c in lines)}
        ok = [c for k, lines in groups.items() if k not in unsent for c in lines if c.get("outcome") == "ok"]
        if groups and len(unsent) == len(groups):
            first = next(iter(unsent.values()))[0]
            return "not-run", None, f"every call group in {state} not run: {first.get('reason') or 'no reason given'}"
        if not ok:
            return "missing", None, f"no ok call in {state}"
        value = int(all(c.get("_judged") in ("correct", "reference", "unjudged") for c in ok))
        if unsent:
            names = ", ".join(f"{k[0]}/{k[2]}" for k in unsent)
            return "value", value, (f"over the {len(groups) - len(unsent)} call group(s) that ran; "
                                    f"{len(unsent)} not run: {names}")
        return "value", value, ""

    stmt_cost = statement_cost(calls, cfg)
    maint = maintenance_cost(folds, stamps, cfg, live, problems)
    storage = storage_cost(census, cfg, problems)
    be = break_even(stmt_cost, maint, statements, cfg)
    basis = price_basis(cfg)
    no_basis = [k for k, b in basis.items() if not b["source"]]
    cost_figs = {
        "cost.figures_without_basis": (len(stmt_cost) + len(maint) + len(storage)) if no_basis else 0,
        "cost.statement_figures": len(stmt_cost),
        "cost.break_even_without_frequency": (
            len(be) if cfg["cost"].get("lookups_per_part_month") is None
            or not cfg["cost"].get("lookups_per_part_month_source") else 0),
    }
    figures = run_rows(bands, cfg, live, calls, stamps, folds, census, matrix, cost_figs, truth,
                       lambda c: window_candidates(stamps, statements, truth, c))
    for f in figures:
        if f["verdict"] == "dead_counter":
            problems.add("dead_counter", f"segmentsOpened {f['statement']} {f['state']}")
        elif f["verdict"] in ("missing", "duplicate", "miss"):
            problems.add(f["verdict"], f"{f['id']}" + (f": {f['reason']}" if f["reason"] else ""))
    detail = statement_detail(calls)
    costs = {(s["statement"], s["state"]): s for s in stmt_cost}
    for d in detail:
        c = costs.get((d["statement"], d["state"])) if d["temp"] == "cold" else None
        if c:
            d["dollars_per_call"] = c["dollars_per_call"]
            d["mean_server_cpu_s"] = c["mean_server_cpu_s"]
    states = []
    for s in all_states:
        if s in failed:
            f = failed[s]
            states.append({"state": s, "status": "failed", "reason": live[s], "step": f.get("step"),
                           "exit": f.get("exit"), "log": f.get("log")})
        elif live[s] is not None:
            states.append({"state": s, "status": "not-run", "reason": live[s]})
        else:
            states.append({"state": s, "status": coverage.get(s, "run")})
    incomplete = incomplete_reasons(cfg, live, calls, figures, needles)
    verdict = "fail" if problems.lines else "incomplete" if incomplete else "pass"
    res = {
        "lane": cfg["lane"],
        "verdict": verdict,
        "problems": problems.lines,
        "incomplete": incomplete,
        "not_requested": not_requested(bands, cfg, live, figures),
        "refusal_classes": {k: {"figures": sum(1 for f in figures if f["status"] == k),
                                "calls": sum(1 for t in calls.groups.values() for v in t.values() for c in v
                                             if c.get("outcome") == k)}
                            for k in (GUARD, UNSUPPORTED)},
        "needle_blocks": needles,
        "states": states,
        "figures": figures,
        "statements": detail,
        "warm_served": warm_served_calls(calls),
        "unjudged_calls": calls.unjudged,
        "cost": {
            "prices": basis,
            "statements": stmt_cost,
            "maintenance": maint,
            "storage": storage,
            "break_even": be,
            "lookups_per_part_month_assumed": cfg["cost"].get("lookups_per_part_month"),
            "lookups_per_part_month_source": cfg["cost"].get("lookups_per_part_month_source"),
            "amortise_months": cfg["cost"].get("amortise_months"),
        },
        "stamps": {
            "build": docs.get("build.json"),
            "host": docs.get("host.json"),
            "declare": docs.get("declare.json"),
            "store": docs.get("store.json"),
            "corpus": docs.get("corpus.json"),
            "loads": [{k: v for k, v in line.items() if k != "stdout"} for _, line in sorted(loads.items()) if line],
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
    return 0 if verdict == "pass" else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
