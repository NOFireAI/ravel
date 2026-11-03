#!/usr/bin/env python3
"""JSON work behind run.sh (issue #1385). Standard library only.

    evaltool.py check DIR FIGURES_OUT        validate the corpus, expected
                                             files and fixture; write figure
                                             lines; exit 1 on any finding
    evaltool.py want corpus DIR OUT          write the {figure: value} the
                                             --check figures must equal
    evaltool.py want loaded DIR OUT          the same for --fixture's query-back
    evaltool.py verify WANT FIGURES          every wanted figure present exactly
                                             once and equal; nothing unknown
    evaltool.py offset T0_NS NOW_NS EXPR     resolve a window offset to unix ns
    evaltool.py case-input DIR ID T0_NS NOW_NS PROFILE LABEL PRIOR OUT
                                             write the agent's case input
    evaltool.py tools PROFILE                print the profile's tool names
    evaltool.py mcp-request TOOL ARGS_JSON   print a tools/call JSON-RPC body
    evaltool.py mcp-envelope RESPONSE_FILE   print the envelope of a tools/call
                                             response (JSON or SSE framing)
    evaltool.py sql-request QUERY START_NS END_NS TOKENS
                                             print a /api/v1/sql request body
    evaltool.py sql-scalar RESPONSE_FILE     print the single cell of a SQL
                                             response; exit 1 unless 1x1
    evaltool.py score DIR ID TRANSCRIPT CONTEXT OUT
                                             score one transcript
    evaltool.py summarize RESULTS_DIR OUT    write summary.tsv over record.json
    evaltool.py field JSON_FILE DOTTED.KEY   print one value; exit 1 when absent
    evaltool.py field-opt JSON_FILE DOTTED.KEY
                                             print one value, empty when absent
    evaltool.py kv OUT KEY=VALUE ...         write a flat JSON object
    evaltool.py now                          print the wall clock in unix ns

Exit codes: 0 ok, 1 a finding, 64 usage, 70 an input this tool could not read.
"""

import json
import os
import re
import sys
import time

JOURNEYS = ["A", "B", "C", "D", "E", "F"]
FULL_TOOLS = [
    "ravel_capabilities", "ravel_describe_data", "ravel_find_labels",
    "ravel_explain_query", "ravel_query_sql", "ravel_query_promql",
    "ravel_search_logs", "ravel_get_trace", "ravel_analyze_timeseries",
]
BASELINE_TOOLS = ["ravel_query_sql", "ravel_query_promql", "ravel_capabilities"]
PROFILES = {"baseline": BASELINE_TOOLS, "full": FULL_TOOLS}
BUDGET_KEYS = ["deadline_ms", "max_rows", "max_bytes_scanned", "max_store_requests"]
KNOWN_TAGS = {"no_data", "not_visible", "induces_failure", "evidence_ref_not_served"}
KNOWN_SETUPS = {"late_batch", "storage_fault"}
EMPTY_CLASSES = {"zero_matches", "window_newer_than_watermark", "refused_by_budget",
                 "storage_unavailable", "window_outside_retention"}
FAILURE_CLASSES = {
    "unauthorized", "invalid_argument", "missing_argument", "validation", "unsupported",
    "budget_estimate_exceeds_ceiling", "budget_exceeded", "deadline", "unavailable",
    "snapshot_invalidated", "cursor_expired", "cursor_invalid", "internal",
}
INVALID_QUERY_CLASSES = {"validation", "invalid_argument", "missing_argument", "unsupported"}
SPECIAL_PATTERNS = {"@first_failure_message", "@any_snapshot_id", "@any_snapshot_id_except_prior"}
OFFSET_RE = re.compile(r"^(T0|now)(([+-])(\d+)([smhd]))?$")
UNIT_NS = {"s": 10**9, "m": 60 * 10**9, "h": 3600 * 10**9, "d": 86400 * 10**9}
PLACEHOLDER_RE = re.compile(r"\{(hhmm:[^}]+|prior_snapshot_id|late_label)\}")

# Every envelope status that is not an error (ravel_mcp::envelope::Status).
SUCCESS_STATUSES = ("ok", "ok_bounded", "ok_page")

# Summary columns, each with its unit, in the order summary.tsv writes them.
SUMMARY_COLUMNS = [
    ("case", "id"), ("run", "index"), ("status", "ran|not_run"),
    ("correct", "bool"), ("facts_matched", "count"), ("facts_total", "count"),
    ("evidence_completeness", "ratio 0-1"), ("completed", "bool"),
    ("tool_calls", "count"), ("within_call_bound", "bool"),
    ("invalid_queries", "count"), ("max_retries_per_failure_class", "count"),
    ("profile_violations", "count"), ("budget_violations", "count"),
    ("unrepaired_failure_classes", "list"), ("wall_time_ms", "ms"),
    ("store_requests", "count, server budget.actual.store_requests"),
    ("store_wire_bytes", "bytes, server budget.actual.bytes_scanned (object-store wire bytes)"),
    ("calls_without_actual", "count"),
    ("response_bytes", "bytes, agent-reported HTTP response bodies"),
]


def die(code, msg):
    sys.stderr.write("evaltool: %s\n" % msg)
    sys.exit(code)


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError) as e:
        die(70, "cannot read %s: %s" % (path, e))


def resolve_offset(expr, t0_ns, now_ns):
    m = OFFSET_RE.match(expr)
    if not m:
        raise ValueError("window offset %r is not T0[+-N(s|m|h|d)] or now[+-N(s|m|h|d)]" % expr)
    base = t0_ns if m.group(1) == "T0" else now_ns
    if m.group(2):
        delta = int(m.group(4)) * UNIT_NS[m.group(5)]
        base = base + delta if m.group(3) == "+" else base - delta
    return base


def hhmm(ns):
    return time.strftime("%H:%M", time.gmtime(ns // 10**9))


def substitute(pattern, ctx):
    def repl(m):
        key = m.group(1)
        if key.startswith("hhmm:"):
            return re.escape(hhmm(resolve_offset(key[5:], ctx["t0_ns"], ctx["now_ns"])))
        return re.escape(ctx.get(key) or "")
    return PLACEHOLDER_RE.sub(repl, pattern)


def walk(obj, dotted):
    for part in dotted.split("."):
        if not isinstance(obj, dict) or part not in obj:
            return False
        obj = obj[part]
    return True


# --- check ------------------------------------------------------------------


def check(root, figures_out):
    errors = []
    corpus_dir = os.path.join(root, "corpus")
    expected_dir = os.path.join(root, "expected")
    fixture = load_json(os.path.join(root, "fixture", "fixture.json"))

    def files(d):
        if not os.path.isdir(d):
            errors.append("%s is not a directory" % d)
            return []
        return sorted(n for n in os.listdir(d) if n.endswith(".json"))

    cases = {}
    seen_at = {}
    per_journey = dict((j, 0) for j in JOURNEYS)
    tagged = {"no_data": [], "not_visible": []}
    for name in files(corpus_dir):
        path = os.path.join("corpus", name)
        try:
            with open(os.path.join(corpus_dir, name)) as f:
                case = json.load(f)
        except ValueError as e:
            errors.append("%s: not valid JSON: %s" % (path, e))
            continue
        cid = case.get("id")
        if not isinstance(cid, str) or not cid:
            errors.append("%s: no id" % path)
            continue
        if cid in seen_at:
            errors.append("duplicate case id %s in %s and %s" % (cid, seen_at[cid], path))
            continue
        seen_at[cid] = path
        if name != cid + ".json":
            errors.append("%s: id %s does not match its file name" % (path, cid))
        journey = case.get("journey")
        if journey not in JOURNEYS:
            errors.append("%s: journey %r is not one of %s" % (path, journey, "".join(JOURNEYS)))
        else:
            per_journey[journey] += 1
        if not isinstance(case.get("question"), str) or not case["question"].strip():
            errors.append("%s: no question" % path)
        window = case.get("window")
        if not isinstance(window, dict) or set(window) != {"from", "to"}:
            errors.append("%s: window must have exactly from and to" % path)
        else:
            try:
                lo = resolve_offset(window["from"], 10**18, 2 * 10**18)
                hi = resolve_offset(window["to"], 10**18, 2 * 10**18)
                if lo >= hi:
                    errors.append("%s: window from is not before to" % path)
            except (ValueError, TypeError) as e:
                errors.append("%s: %s" % (path, e))
        budget = case.get("budget")
        if not isinstance(budget, dict) or sorted(budget) != sorted(BUDGET_KEYS):
            errors.append("%s: budget must have exactly %s" % (path, ", ".join(BUDGET_KEYS)))
        else:
            for k in BUDGET_KEYS:
                v = budget[k]
                if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v <= 0):
                    errors.append("%s: budget.%s must be a positive integer or null" % (path, k))
            if budget["deadline_ms"] is None:
                errors.append("%s: budget.deadline_ms must be set" % path)
        tags = case.get("tags")
        if not isinstance(tags, list) or not set(tags) <= KNOWN_TAGS:
            errors.append("%s: tags must be a list drawn from %s" % (path, sorted(KNOWN_TAGS)))
            tags = []
        for t in ("no_data", "not_visible"):
            if t in tags:
                tagged[t].append(cid)
        setup = case.get("setup")
        if setup is not None and setup not in KNOWN_SETUPS:
            errors.append("%s: setup %r is not one of %s" % (path, setup, sorted(KNOWN_SETUPS)))
        if "{late_label}" in case.get("question", "") and setup != "late_batch":
            errors.append("%s: question names {late_label} without setup late_batch" % path)
        cases[cid] = case

    expected_ids = set()
    for name in files(expected_dir):
        path = os.path.join("expected", name)
        try:
            with open(os.path.join(expected_dir, name)) as f:
                exp = json.load(f)
        except ValueError as e:
            errors.append("%s: not valid JSON: %s" % (path, e))
            continue
        eid = exp.get("id")
        if name != "%s.json" % eid:
            errors.append("%s: id %r does not match its file name" % (path, eid))
            continue
        expected_ids.add(eid)
        if eid not in cases:
            errors.append("%s: no corpus case %s for this expected file" % (path, eid))
            continue
        errors.extend(check_expected(path, exp, cases[eid], fixture))

    for cid in sorted(cases):
        if cid not in expected_ids:
            errors.append("case %s: no expected file expected/%s.json" % (cid, cid))
    for j in JOURNEYS:
        if per_journey[j] < 3:
            errors.append("journey %s has %d cases; at least 3 required" % (j, per_journey[j]))
    if len(cases) < 24:
        errors.append("corpus has %d cases; at least 24 required" % len(cases))
    for t in ("no_data", "not_visible"):
        if not tagged[t]:
            errors.append("no case is tagged %s; at least one required" % t)

    errors.extend(check_fixture(root, fixture))

    lines = ["figure corpus.cases=%d" % len(cases),
             "figure corpus.expected_files=%d" % len(expected_ids)]
    for j in JOURNEYS:
        lines.append("figure corpus.journey_%s=%d" % (j, per_journey[j]))
    lines.append("figure corpus.no_data_cases=%d" % len(tagged["no_data"]))
    lines.append("figure corpus.not_visible_cases=%d" % len(tagged["not_visible"]))
    for k, v in sorted(fixture_counts(root).items()):
        lines.append("figure fixture.%s=%d" % (k, v))
    with open(figures_out, "w") as f:
        f.write("\n".join(lines) + "\n")
    sys.stdout.write("no_data cases: %s\nnot_visible cases: %s\n"
                     % (" ".join(sorted(tagged["no_data"])), " ".join(sorted(tagged["not_visible"]))))
    for e in errors:
        sys.stderr.write("check: %s\n" % e)
    return 1 if errors else 0


def check_expected(path, exp, case, fixture):
    errors = []
    dummy = {"t0_ns": 10**18, "now_ns": 2 * 10**18, "prior_snapshot_id": "x", "late_label": "x"}
    for field in ("answer_facts", "evidence"):
        items = exp.get(field)
        if not isinstance(items, list) or (field == "answer_facts" and not items):
            errors.append("%s: %s must be a list%s" % (path, field,
                                                      " with at least one fact" if field == "answer_facts" else ""))
            continue
        label = "fact" if field == "answer_facts" else "item"
        for it in items:
            if not isinstance(it, dict) or not it.get(label) or not isinstance(it.get("pattern"), str):
                errors.append("%s: every %s entry needs %s and pattern" % (path, field, label))
                continue
            pat = it["pattern"]
            if pat.startswith("@"):
                if pat not in SPECIAL_PATTERNS:
                    errors.append("%s: unknown special pattern %s" % (path, pat))
                continue
            if "{prior_snapshot_id}" in pat or "{late_label}" in pat:
                if case.get("setup") != "late_batch":
                    errors.append("%s: pattern %s needs setup late_batch" % (path, pat))
            try:
                re.compile(substitute(pat, dummy), re.I)
            except (re.error, ValueError) as e:
                errors.append("%s: pattern %r does not compile: %s" % (path, pat, e))
    tools = exp.get("tools_expected")
    if not isinstance(tools, list) or not tools or not set(tools) <= set(FULL_TOOLS):
        errors.append("%s: tools_expected must be a non-empty list of full-profile tools" % path)
    bound = exp.get("call_bound")
    if not isinstance(bound, int) or isinstance(bound, bool) or bound <= 0:
        errors.append("%s: call_bound must be a positive integer" % path)
    refs = exp.get("fixture_refs")
    if not isinstance(refs, list) or not refs:
        errors.append("%s: fixture_refs must be a non-empty list" % path)
    else:
        for r in refs:
            if not walk(fixture, r):
                errors.append("%s: fixture_ref %s is not in fixture/fixture.json" % (path, r))
    classes = exp.get("expected_failure_classes")
    if classes is not None and (not isinstance(classes, list) or not set(classes) <= FAILURE_CLASSES):
        errors.append("%s: expected_failure_classes must name known failure classes" % path)
    journey = case.get("journey")
    if journey == "D":
        if not classes:
            errors.append("%s: a journey D case needs expected_failure_classes" % path)
        if exp.get("retry_bound") != 1:
            errors.append("%s: a journey D case needs retry_bound 1" % path)
    if journey == "E" and exp.get("evidence_ref_served") is not False:
        errors.append("%s: a journey E case must state evidence_ref_served false" % path)
    if journey == "F" and exp.get("expected_empty_class") not in EMPTY_CLASSES:
        errors.append("%s: a journey F case needs expected_empty_class from %s" % (path, sorted(EMPTY_CLASSES)))
    return errors


def gen_json(root, sub):
    import subprocess
    gen = os.path.join(root, "fixture", "gen.py")
    try:
        out = subprocess.check_output([sys.executable, gen, sub])
    except (OSError, subprocess.CalledProcessError) as e:
        die(70, "fixture/gen.py %s failed: %s" % (sub, e))
    return json.loads(out)


def fixture_counts(root):
    return gen_json(root, "counts")


def check_fixture(root, fixture):
    errors = []
    ent = gen_json(root, "entities")
    fe = fixture.get("entities", {})
    if sorted(fe.get("incident", {}).get("failing_trace_ids", [])) != ent["failing_trace_ids"]:
        errors.append("fixture.json failing_trace_ids differ from gen.py entities")
    if sorted(fe.get("security", {}).get("user_trace_ids", [])) != ent["user_trace_ids"]:
        errors.append("fixture.json user_trace_ids differ from gen.py entities")
    if fe.get("security", {}).get("user_id") != ent["user_id"]:
        errors.append("fixture.json user_id differs from gen.py entities")
    if fe.get("services") != ent["services"]:
        errors.append("fixture.json services differ from gen.py entities")
    return errors


# --- want / verify ----------------------------------------------------------


LOADED_KEYS = ["samples", "series", "log_rows", "error_log_rows", "target_user_log_rows",
               "spans", "traces", "error_spans"]


def want(kind, root, out):
    fixture = load_json(os.path.join(root, "fixture", "fixture.json"))
    counts = fixture.get("counts", {})
    result = {}
    if kind == "corpus":
        bands = load_json(os.path.join(root, "bands.json"))
        for k, v in sorted(bands.get("corpus", {}).items()):
            result["corpus." + k] = v
        for k, v in sorted(counts.items()):
            result["fixture." + k] = v
    elif kind == "loaded":
        for k in LOADED_KEYS:
            if k not in counts:
                die(70, "fixture.json counts has no %s" % k)
            result["fixture." + k] = counts[k]
    else:
        die(64, "want takes corpus or loaded")
    with open(out, "w") as f:
        json.dump(result, f, sort_keys=True)
    return 0


def verify(want_path, figures_path):
    wanted = load_json(want_path)
    seen = {}
    errors = []
    try:
        with open(figures_path) as f:
            lines = f.read().splitlines()
    except OSError as e:
        die(70, "cannot read %s: %s" % (figures_path, e))
    for line in lines:
        m = re.match(r"^figure ([A-Za-z0-9_.]+)=(-?\d+)$", line)
        if not m:
            if line.strip():
                errors.append("unparseable figure line: %r" % line)
            continue
        seen.setdefault(m.group(1), []).append(int(m.group(2)))
    for key in sorted(wanted):
        got = seen.get(key, [])
        if not got:
            errors.append("figure %s missing; want %d" % (key, wanted[key]))
        elif len(got) > 1:
            errors.append("figure %s printed %d times; want exactly once" % (key, len(got)))
        elif got[0] != wanted[key]:
            errors.append("figure %s=%d outside its band; want exactly %d" % (key, got[0], wanted[key]))
    for key in sorted(set(seen) - set(wanted)):
        errors.append("figure %s has no stated band" % key)
    for e in errors:
        sys.stderr.write("verify: %s\n" % e)
    if not errors:
        sys.stdout.write("verify: %d figures, each present exactly once and equal to its band\n"
                         % len(wanted))
    return 1 if errors else 0


# --- run support ------------------------------------------------------------


def case_input(root, cid, t0_ns, now_ns, profile, label, prior_path, out):
    case = load_json(os.path.join(root, "corpus", cid + ".json"))
    question = case["question"].replace("{late_label}", label)
    doc = {
        "id": cid,
        "journey": case["journey"],
        "question": question,
        "time_range": {
            "start_ns": str(resolve_offset(case["window"]["from"], t0_ns, now_ns)),
            "end_ns": str(resolve_offset(case["window"]["to"], t0_ns, now_ns)),
        },
        "budget": case["budget"],
        "profile": profile,
        "tools": PROFILES[profile],
    }
    if prior_path != "-":
        doc["prior_result"] = load_json(prior_path)
    with open(out, "w") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    return 0


def mcp_request(tool, args_json):
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": tool, "arguments": json.loads(args_json),
            "_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28",
                      "io.modelcontextprotocol/clientCapabilities": {}},
        },
    }
    sys.stdout.write(json.dumps(body))
    return 0


def parse_rpc(text):
    text = text.strip()
    if text.startswith("{"):
        return json.loads(text)
    data = [ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")]
    for chunk in reversed(data):
        if chunk.startswith("{"):
            obj = json.loads(chunk)
            if "result" in obj or "error" in obj:
                return obj
    raise ValueError("no JSON-RPC result in the response")


def mcp_envelope(path):
    try:
        with open(path) as f:
            rpc = parse_rpc(f.read())
    except (OSError, ValueError) as e:
        die(70, "cannot read the MCP response %s: %s" % (path, e))
    if "error" in rpc:
        die(1, "MCP protocol error: %s" % json.dumps(rpc["error"]))
    env = rpc.get("result", {}).get("structuredContent")
    if not isinstance(env, dict):
        die(70, "MCP result carries no structuredContent")
    sys.stdout.write(json.dumps(env))
    return 0


def sql_request(query, start_ns, end_ns, tokens):
    body = {"query": query, "start": int(start_ns) / 1e9, "end": int(end_ns) / 1e9}
    toks = [t.strip() for t in tokens.split(",") if t.strip()]
    if toks:
        body["min_commit_token"] = toks
    sys.stdout.write(json.dumps(body))
    return 0


def sql_scalar(path):
    doc = load_json(path)
    if doc.get("status") != "success":
        die(1, "SQL request failed: %s" % json.dumps(doc)[:400])
    rows = doc.get("data", {}).get("rows")
    if not isinstance(rows, list) or len(rows) != 1 or len(rows[0]) != 1:
        die(1, "SQL response is not exactly one cell: %s" % json.dumps(rows)[:400])
    cell = rows[0][0]
    try:
        sys.stdout.write("%d\n" % int(cell))
    except (TypeError, ValueError):
        die(1, "SQL cell %r is not an integer" % (cell,))
    return 0


# --- score ------------------------------------------------------------------


def score(root, cid, transcript_path, context_path, out):
    exp = load_json(os.path.join(root, "expected", cid + ".json"))
    ctx = load_json(context_path)
    record = {
        "case": cid, "profile": ctx["profile"], "condition": ctx["condition"],
        "run": ctx["run"], "ravel_sha": ctx["ravel_sha"], "wall_time_ms": ctx["wall_time_ms"],
        "agent_exit": ctx["agent_exit"], "status": "ran",
    }
    try:
        with open(transcript_path) as f:
            tr = json.load(f)
    except (OSError, ValueError) as e:
        tr = None
        record["transcript_error"] = str(e)
    if not isinstance(tr, dict):
        tr = {}
    calls = tr.get("calls") if isinstance(tr.get("calls"), list) else []
    answer = tr.get("answer") if isinstance(tr.get("answer"), str) else ""
    record["model"] = tr.get("model")
    record["model_version"] = tr.get("model_version")
    envs = [c.get("response") if isinstance(c.get("response"), dict) else {} for c in calls]
    failures = [(i, (e.get("failure") or {})) for i, e in enumerate(envs)
                if e.get("status") == "error" or e.get("failure")]
    snapshots = sorted({(e.get("visibility") or {}).get("snapshot_id") for e in envs} - {None, ""})
    prior = ctx.get("prior_snapshot_id") or ""

    def matches(pat):
        if pat == "@first_failure_message":
            msg = failures[0][1].get("message") if failures else None
            return bool(msg) and msg in answer
        if pat == "@any_snapshot_id":
            return any(s in answer for s in snapshots)
        if pat == "@any_snapshot_id_except_prior":
            return any(s in answer for s in snapshots if s != prior)
        sub = substitute(pat, ctx)
        if "{prior_snapshot_id}" in pat and not prior:
            return False
        return re.search(sub, answer, re.I) is not None

    facts = [matches(f["pattern"]) for f in exp["answer_facts"]]
    evidence = [matches(e["pattern"]) for e in exp["evidence"]]
    record["facts_matched"] = sum(facts)
    record["facts_total"] = len(facts)
    record["facts_missed"] = [f["fact"] for f, ok in zip(exp["answer_facts"], facts) if not ok]
    record["correct"] = all(facts) and bool(answer)
    record["evidence_completeness"] = (round(sum(evidence) / float(len(evidence)), 4)
                                       if evidence else None)
    record["evidence_missed"] = [e["item"] for e, ok in zip(exp["evidence"], evidence) if not ok]
    record["completed"] = (ctx["agent_exit"] == 0 and tr.get("completed") is True and bool(answer))
    record["tool_calls"] = len(calls)
    record["call_bound"] = exp["call_bound"]
    record["within_call_bound"] = len(calls) <= exp["call_bound"]
    allowed = set(PROFILES[ctx["profile"]])
    record["profile_violations"] = sum(1 for c in calls if c.get("tool") not in allowed)
    classes = [f.get("class") for _, f in failures]
    record["failure_classes"] = classes
    record["invalid_queries"] = sum(1 for c in classes if c in INVALID_QUERY_CLASSES)
    record["retries_per_failure_class"] = retries(calls, envs)
    vals = [v for v in record["retries_per_failure_class"].values() if v is not None]
    record["max_retries_per_failure_class"] = max(vals) if vals else 0
    record["unrepaired_failure_classes"] = sorted(
        k for k, v in record["retries_per_failure_class"].items() if v is None)
    if "retry_bound" in exp:
        record["retry_bound"] = exp["retry_bound"]
        record["expected_failure_seen"] = any(c in exp.get("expected_failure_classes", [])
                                              for c in classes)
    case = load_json(os.path.join(root, "corpus", cid + ".json"))
    record["budget_violations"] = budget_violations(calls, case.get("budget") or {})
    req = byt = without = 0
    for e in envs:
        actual = (e.get("budget") or {}).get("actual")
        if isinstance(actual, dict) and "store_requests" in actual:
            req += int(actual.get("store_requests") or 0)
            byt += int(actual.get("bytes_scanned") or 0)
        else:
            without += 1
    record["store_requests"] = req
    record["store_wire_bytes"] = byt
    record["calls_without_actual"] = without
    record["store_source"] = ("sum of budget.actual.store_requests and budget.actual.bytes_scanned "
                              "(object-store wire bytes, server-counted) over the calls that carry "
                              "an actual block; calls_without_actual counts the rest, absent not zero")
    record["response_bytes"] = sum(int(c.get("response_bytes") or 0) for c in calls)
    record["response_bytes_source"] = "agent-reported byte length of each HTTP response body"
    with open(out, "w") as f:
        json.dump(record, f, indent=1, sort_keys=True)
    return 0


def retries(calls, envs):
    """Per failure class: calls after its first occurrence until a call of the
    same tool succeeds; None when no such call succeeds."""
    out = {}
    for i, e in enumerate(envs):
        f = e.get("failure") or {}
        cls = f.get("class") if (e.get("status") == "error" or f) else None
        if not cls or cls in out:
            continue
        tool = calls[i].get("tool")
        n = 0
        repaired = None
        for j in range(i + 1, len(calls)):
            if calls[j].get("tool") != tool:
                continue
            n += 1
            if envs[j].get("status") in SUCCESS_STATUSES:
                repaired = n
                break
        out[cls] = repaired
    return out


SCAN_BUDGET_TOOLS = {"ravel_query_sql", "ravel_query_promql", "ravel_search_logs", "ravel_get_trace"}
BUDGET_TOOLS = {
    "max_bytes_scanned": SCAN_BUDGET_TOOLS,
    "max_store_requests": SCAN_BUDGET_TOOLS,
    "max_rows": SCAN_BUDGET_TOOLS | {"ravel_analyze_timeseries"},
}


def budget_violations(calls, budget):
    """Calls to a tool that takes a budget the case sets, sent without it or above it."""
    n = 0
    for c in calls:
        args = c.get("arguments") if isinstance(c.get("arguments"), dict) else {}
        for k, tools in sorted(BUDGET_TOOLS.items()):
            v = budget.get(k)
            if v is None or c.get("tool") not in tools:
                continue
            got = args.get(k)
            if isinstance(got, bool) or not isinstance(got, int) or got > v:
                n += 1
    return n


def cell(value):
    """One summary.tsv cell: empty for an absent figure, JSON for a list."""
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


def summarize(results_dir, out):
    rows = []
    for dirpath, _dirs, names in os.walk(results_dir):
        if "record.json" in names:
            rows.append(load_json(os.path.join(dirpath, "record.json")))
    rows.sort(key=lambda r: (r.get("run", 0), r.get("case", "")))
    with open(out, "w") as f:
        f.write("# units: " + "; ".join("%s=%s" % c for c in SUMMARY_COLUMNS) + "\n")
        f.write("\t".join(c for c, _ in SUMMARY_COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(cell(r.get(c)) for c, _ in SUMMARY_COLUMNS) + "\n")
    sys.stdout.write("summary: %d records in %s\n" % (len(rows), out))
    return 0


def field(path, dotted, required=True):
    obj = load_json(path)
    for part in dotted.split("."):
        obj = obj.get(part) if isinstance(obj, dict) else None
    if obj is None:
        if required:
            sys.stderr.write("%s: %s is absent\n" % (path, dotted))
            return 1
        return 0
    sys.stdout.write((obj if isinstance(obj, str) else json.dumps(obj)) + "\n")
    return 0


def write_kv(out, pairs):
    doc = {}
    for p in pairs:
        k, sep, v = p.partition("=")
        if not sep or not k:
            die(64, "kv takes KEY=VALUE pairs, got %r" % p)
        doc[k] = int(v) if re.match(r"^-?\d+$", v) else v
    with open(out, "w") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    return 0


def main(argv):
    a = argv[1:]
    try:
        if a[:1] == ["check"] and len(a) == 3:
            return check(a[1], a[2])
        if a[:1] == ["want"] and len(a) == 4:
            return want(a[1], a[2], a[3])
        if a[:1] == ["verify"] and len(a) == 3:
            return verify(a[1], a[2])
        if a[:1] == ["offset"] and len(a) == 4:
            sys.stdout.write("%d\n" % resolve_offset(a[3], int(a[1]), int(a[2])))
            return 0
        if a[:1] == ["case-input"] and len(a) == 9:
            return case_input(a[1], a[2], int(a[3]), int(a[4]), a[5], a[6], a[7], a[8])
        if a[:1] == ["tools"] and len(a) == 2 and a[1] in PROFILES:
            sys.stdout.write("\n".join(PROFILES[a[1]]) + "\n")
            return 0
        if a[:1] == ["mcp-request"] and len(a) == 3:
            return mcp_request(a[1], a[2])
        if a[:1] == ["mcp-envelope"] and len(a) == 2:
            return mcp_envelope(a[1])
        if a[:1] == ["sql-request"] and len(a) == 5:
            return sql_request(a[1], a[2], a[3], a[4])
        if a[:1] == ["sql-scalar"] and len(a) == 2:
            return sql_scalar(a[1])
        if a[:1] == ["score"] and len(a) == 6:
            return score(a[1], a[2], a[3], a[4], a[5])
        if a[:1] == ["summarize"] and len(a) == 3:
            return summarize(a[1], a[2])
        if a[:1] == ["field"] and len(a) == 3:
            return field(a[1], a[2])
        if a[:1] == ["field-opt"] and len(a) == 3:
            return field(a[1], a[2], required=False)
        if a[:1] == ["kv"] and len(a) >= 2:
            return write_kv(a[1], a[2:])
        if a[:1] == ["now"] and len(a) == 1:
            sys.stdout.write("%d\n" % time.time_ns())
            return 0
    except ValueError as e:
        die(64, str(e))
    sys.stderr.write(__doc__)
    return 64


if __name__ == "__main__":
    sys.exit(main(sys.argv))
