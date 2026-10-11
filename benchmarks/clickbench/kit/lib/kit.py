#!/usr/bin/env python3
"""Shared helpers for the ClickBench measurement kit (ADR-2867 change 2).

launch.sh and fetch.sh run this on the laptop and box/run.sh runs it on the
box, so it uses the standard library only and runs on Python 3.6 (no
tomllib): both macOS's python3 and Amazon Linux 2023's are older than 3.11.

Subcommands (each exits 0 on success, 2 on a refusal, 64 on bad usage):

  get FILE KEY                 print one run.toml value; an array prints one
                               item per line; a dotted key reads a table
  check-run FILE               refuse a run.toml missing a required key or
                               holding a malformed SHA, lane or phase
  check-bands FILE             refuse a band file holding a placeholder value,
                               naming every key that holds one
  request-bodies CORPUS DIR    write DIR/NN.json ({"query": sql}) and
                               DIR/index.tsv (NN, statement id) per statement
  phases OUT PHASE...          print per-phase call and error counts from
                               OUT/<phase>/calls.jsonl as JSON
  assemble ARGS...             write results.json from the box's phase output
  validate FILE                refuse a results.json missing a field the
                               ClickBenchParquetReport shape or the kit's
                               provenance and bands blocks require
  absolute LABEL FILE          print a results.json's absolute band table,
                               exit 1 on a miss
  relative BANDS A.json B.json evaluate the relative bands, print the table,
                               exit 1 on a miss

Band files are the lane's pre-registration TOML (parse_prereg in
crates/ravel-bench/src/clickbench_parquet/report.rs reads the same file and
refuses any key it does not know), so the kit's own bands are written as
comment lines that TOML ignores:

  #@ relative.<band> = <number>
  #@ absolute.<metric>_max = <number>    (or _min)

Every plain TOML key of the parquet lane's file is an absolute band as well;
`absolute_bands` below names how each is judged.
"""

import json
import math
import os
import re
import sys

PLACEHOLDER_PREFIX = "FILL-IN"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")

# Phases box/run.sh implements. T5 adds its phases here and in run.sh.
KNOWN_PHASES = ("benchmark",)

RUN_REQUIRED = (
    "sha_a", "sha_b", "lanes", "phases", "instance_type", "region", "bucket",
    "prefix", "instance_profile", "repo_url", "tenant", "shards", "corpus",
    "dataset_url", "rustfs_url", "rustfs_sha256", "memory_budget_bytes",
    "audit_mode",
)

# ClickBenchParquetReport's fields (report.rs, `pub struct
# ClickBenchParquetReport`), its Provenance's and StatementFigures', none of
# which is optional to serde.
REPORT_FIELDS = (
    "provenance", "statements", "totals", "registered_failures_answered",
    "concurrency", "concurrency_error",
)
PROVENANCE_FIELDS = (
    "git_sha", "binary", "arm", "location", "server", "server_log",
    "server_settings", "sql_max_query_bytes", "sql_tenant_max_bytes",
    "reference_dir", "reference_version", "ddl_files",
)
STATEMENT_FIELDS = (
    "number", "cold_s", "hot_s", "verdict", "float_mismatches",
    "missing_rows", "extra_rows", "compare_error", "error",
)
TOTALS_FIELDS = ("cold_sum_s", "hot_sum_s", "answered", "failed")
# The kit's own provenance stamps.
KIT_STAMPS = (
    "kit_arm", "run_id", "lanes", "toolchain", "hardware", "backend",
    "corpus_digest", "server_flags", "audit_mode", "burst_balance", "fold",
    "object_counts",
)
STAMPED_SETTINGS = (
    "cache_max_bytes", "catalog_cache_max_bytes", "fetch_concurrency",
    "sql_max_query_bytes", "sql_tenant_max_bytes",
)


class Refusal(Exception):
    pass


# --- TOML subset -----------------------------------------------------------

def _strip_comment(line):
    out, quote = [], None
    for ch in line:
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
            out.append(ch)
        elif ch == "#":
            break
        else:
            out.append(ch)
    return "".join(out).strip()


def _value(text, where):
    text = text.strip()
    if text.startswith("["):
        if not text.endswith("]"):
            raise Refusal("%s: arrays must sit on one line" % where)
        inner = text[1:-1].strip()
        if not inner:
            return []
        items, buf, quote = [], "", None
        for ch in inner:
            if quote:
                buf += ch
                if ch == quote:
                    quote = None
            elif ch in "\"'":
                quote = ch
                buf += ch
            elif ch == ",":
                items.append(buf)
                buf = ""
            else:
                buf += ch
        if buf.strip():
            items.append(buf)
        return [_value(i, where) for i in items]
    if len(text) >= 2 and text[0] == text[-1] and text[0] in "\"'":
        return text[1:-1]
    if text in ("true", "false"):
        return text == "true"
    try:
        if re.match(r"^[+-]?[0-9_]+$", text):
            return int(text.replace("_", ""))
        return float(text.replace("_", ""))
    except ValueError:
        raise Refusal("%s: cannot read value %r" % (where, text))


def parse_toml(path):
    """The flat subset run.toml and the band files use: [table] headers,
    `key = value` with strings, numbers, booleans and one-line arrays. Kit
    band lines (`#@ ...`) are returned separately."""
    data, kit_lines, table = {}, {}, None
    try:
        lines = open(path).read().splitlines()
    except OSError as e:
        raise Refusal("cannot read %s: %s" % (path, e))
    for n, raw in enumerate(lines, 1):
        where = "%s:%d" % (path, n)
        stripped = raw.strip()
        if stripped.startswith("#@"):
            body = stripped[2:].strip()
            if "=" not in body:
                raise Refusal("%s: a #@ line must read `#@ key = value`" % where)
            key, val = body.split("=", 1)
            key = key.strip()
            if key in kit_lines:
                raise Refusal("%s: %s is set twice" % (where, key))
            kit_lines[key] = _value(val, where)
            continue
        line = _strip_comment(raw)
        if not line:
            continue
        if line.startswith("[") and line.endswith("]"):
            table = line[1:-1].strip()
            if table in data:
                raise Refusal("%s: table [%s] is declared twice" % (where, table))
            data[table] = {}
            continue
        if "=" not in line:
            raise Refusal("%s: expected `key = value`" % where)
        key, val = line.split("=", 1)
        key = key.strip().strip('"')
        target = data if table is None else data[table]
        if key in target:
            raise Refusal("%s: %s is set twice" % (where, key))
        target[key] = _value(val, where)
    return data, kit_lines


def lookup(data, key):
    node = data
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


# --- run.toml --------------------------------------------------------------

def check_run(path):
    data, _ = parse_toml(path)
    missing = [k for k in RUN_REQUIRED if lookup(data, k) in (None, "")]
    if missing:
        raise Refusal("run.toml %s: missing required key(s): %s" % (path, ", ".join(missing)))
    for key in ("sha_a", "sha_b"):
        if not SHA_RE.match(str(data[key])):
            raise Refusal("run.toml %s: %s must be a full 40-character commit, got %r"
                          % (path, key, data[key]))
    for key in ("lanes", "phases"):
        if not isinstance(data[key], list) or not data[key]:
            raise Refusal("run.toml %s: %s must be a non-empty array" % (path, key))
        for item in data[key]:
            if not isinstance(item, str) or not NAME_RE.match(item):
                raise Refusal("run.toml %s: %s holds %r, not a name" % (path, key, item))
    for phase in data["phases"]:
        if phase not in KNOWN_PHASES:
            raise Refusal("run.toml %s: phase %r is not one box/run.sh implements (%s)"
                          % (path, phase, ", ".join(KNOWN_PHASES)))
    bands = data.get("bands")
    for lane in data["lanes"]:
        if not isinstance(bands, dict) or not isinstance(bands.get(lane), str):
            raise Refusal("run.toml %s: lane %r has no band file under [bands]" % (path, lane))
    for key in ("shards", "memory_budget_bytes"):
        if not isinstance(data[key], int) or isinstance(data[key], bool) or data[key] <= 0:
            raise Refusal("run.toml %s: %s must be a whole number above 0" % (path, key))
    if data["audit_mode"] not in ("off", "required"):
        raise Refusal("run.toml %s: audit_mode must be off or required" % path)
    if data.get("audit_text", "redacted") not in ("redacted", "plaintext"):
        raise Refusal("run.toml %s: audit_text must be redacted or plaintext" % path)
    if data.get("audit_text") == "plaintext" and data["audit_mode"] != "required":
        raise Refusal("run.toml %s: audit_text plaintext needs audit_mode required "
                      "(the server refuses it otherwise)" % path)
    return data


# --- band files ------------------------------------------------------------

def check_bands(path):
    data, kit = parse_toml(path)
    found = []

    def walk(prefix, node):
        if isinstance(node, dict):
            for k, v in node.items():
                walk(prefix + [k], v)
        elif isinstance(node, list):
            for v in node:
                walk(prefix, v)
        elif isinstance(node, str) and node.startswith(PLACEHOLDER_PREFIX):
            found.append(".".join(prefix))

    walk([], data)
    walk([], kit)
    if found:
        raise Refusal("band file %s: placeholder value in %s; fill it in before launching"
                      % (path, ", ".join(sorted(set(found)))))
    return data, kit


def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def absolute_bands(bands_path, report):
    """Pass, miss or inconclusive for every absolute band in the lane's file,
    judged against one arm's report."""
    data, kit = check_bands(bands_path)
    totals = report["totals"]
    hot, cold = totals["hot_sum_s"], totals["cold_sum_s"]
    failed = set(totals["failed"])
    out = []

    def add(band, value, bound, verdict, note=""):
        out.append({"band": band, "kind": "absolute", "value": value,
                    "bound": bound, "verdict": verdict, "note": note})

    stamped = report["provenance"].get("sql_max_query_bytes")
    if "memory_cap_bytes" in data:
        cap = data["memory_cap_bytes"]
        add("sql_max_query_bytes == memory_cap_bytes", stamped, cap,
            "pass" if stamped == cap else "miss")
    if "arm_b_hot_s" in data:
        bound = 1.25 * data["arm_b_hot_s"]
        add("hot_sum_s <= 1.25 x arm_b_hot_s", hot, bound, "pass" if hot <= bound else "miss")
    if "arm_b_cold_s" in data:
        bound = 1.25 * data["arm_b_cold_s"]
        add("cold_sum_s <= 1.25 x arm_b_cold_s", cold, bound, "pass" if cold <= bound else "miss")
    if "rlog_hot_ceiling_s" in data:
        bound = data["rlog_hot_ceiling_s"]
        add("hot_sum_s < rlog_hot_ceiling_s", hot, bound, "pass" if hot < bound else "miss")
    if "failures" in data:
        allowed = set()
        for label in data["failures"]:
            allowed.add(int(str(label).lstrip("q")))
        extra = sorted(failed - allowed)
        add("failed statements within failures",
            ["q%d" % n for n in sorted(failed)],
            sorted(data["failures"]),
            "miss" if extra else "pass",
            ("outside failures: %s" % ", ".join("q%d" % n for n in extra)) if extra else "")
    if "concurrency_qps_floor" in data:
        conc = report.get("concurrency")
        qps = conc.get("qps") if isinstance(conc, dict) else None
        if _num(qps):
            add("concurrency qps >= concurrency_qps_floor", qps, data["concurrency_qps_floor"],
                "pass" if qps >= data["concurrency_qps_floor"] else "miss")
        else:
            add("concurrency qps >= concurrency_qps_floor", None, data["concurrency_qps_floor"],
                "inconclusive", "no concurrency phase ran")

    metrics = run_metrics(report)
    for key, bound in sorted(kit.items()):
        if not key.startswith("absolute."):
            continue
        name = key[len("absolute."):]
        m = re.match(r"^(.+)_(max|min)$", name)
        if not m or not _num(bound):
            raise Refusal("band file %s: %s must be absolute.<metric>_max|_min = <number>"
                          % (bands_path, key))
        metric, side = m.group(1), m.group(2)
        if metric not in METRICS:
            raise Refusal("band file %s: %s names unknown metric %r (known: %s)"
                          % (bands_path, key, metric, ", ".join(METRICS)))
        value = metrics.get(metric)
        label = "%s %s %s" % (metric, "<=" if side == "max" else ">=", bound)
        if not _num(value):
            add(label, None, bound, "inconclusive", "metric not measured")
        elif side == "max":
            add(label, value, bound, "pass" if value <= bound else "miss")
        else:
            add(label, value, bound, "pass" if value >= bound else "miss")
    return out


METRICS = ("hot_sum_s", "cold_sum_s", "error_pooled", "qps")


def run_metrics(report):
    calls = errors = 0
    for phase in (report.get("phases") or {}).values():
        calls += phase.get("calls", 0)
        errors += phase.get("errors", 0)
    conc = report.get("concurrency")
    return {
        "hot_sum_s": report["totals"]["hot_sum_s"],
        "cold_sum_s": report["totals"]["cold_sum_s"],
        "error_pooled": (errors / calls) if calls else None,
        "qps": conc.get("qps") if isinstance(conc, dict) else None,
    }


RELATIVE_BANDS = (
    "error_pooled_max_delta", "qps_min_ratio", "hot_statement_max_ratio",
    "hot_statement_floor_s", "hot_sum_max_ratio", "cold_sum_max_ratio",
)


def relative_bands(bands_path, a, b):
    """Arm B judged against arm A, per the `#@ relative.*` lines."""
    _, kit = check_bands(bands_path)
    rel = {}
    for key, value in kit.items():
        if not key.startswith("relative."):
            continue
        name = key[len("relative."):]
        if name not in RELATIVE_BANDS or not _num(value):
            raise Refusal("band file %s: %s is not a known relative band with a number (%s)"
                          % (bands_path, key, ", ".join(RELATIVE_BANDS)))
        rel[name] = value
    out = []

    def add(band, value, bound, verdict, note=""):
        out.append({"band": band, "kind": "relative", "value": value,
                    "bound": bound, "verdict": verdict, "note": note})

    ma, mb = run_metrics(a), run_metrics(b)
    if "error_pooled_max_delta" in rel:
        d = rel["error_pooled_max_delta"]
        if _num(ma["error_pooled"]) and _num(mb["error_pooled"]):
            bound = ma["error_pooled"] + d
            add("error_pooled <= A + %g" % d, mb["error_pooled"], bound,
                "pass" if mb["error_pooled"] <= bound else "miss")
        else:
            add("error_pooled <= A + %g" % d, None, None, "inconclusive", "no calls recorded")
    if "qps_min_ratio" in rel:
        r = rel["qps_min_ratio"]
        if _num(ma["qps"]) and _num(mb["qps"]):
            bound = r * ma["qps"]
            add("qps >= %g x A" % r, mb["qps"], bound, "pass" if mb["qps"] >= bound else "miss")
        else:
            add("qps >= %g x A" % r, None, None, "inconclusive", "no concurrency phase ran")
    for metric, key in (("hot_sum_s", "hot_sum_max_ratio"), ("cold_sum_s", "cold_sum_max_ratio")):
        if key in rel:
            bound = rel[key] * ma[metric]
            add("%s <= %g x A" % (metric, rel[key]), mb[metric], bound,
                "pass" if mb[metric] <= bound else "miss")
    if "hot_statement_max_ratio" in rel:
        r = rel["hot_statement_max_ratio"]
        floor = rel.get("hot_statement_floor_s", 0.0)
        a_hot = {s["number"]: s["hot_s"] for s in a["statements"]}
        for s in sorted(b["statements"], key=lambda s: s["number"]):
            label = "q%02d hot_s <= max(%g x A, %g s)" % (s["number"], r, floor)
            ah = a_hot.get(s["number"])
            if not _num(ah) or not _num(s["hot_s"]):
                add(label, s["hot_s"], None, "inconclusive",
                    "failed on %s" % ("A" if not _num(ah) else "B"))
                continue
            bound = max(r * ah, floor)
            add(label, s["hot_s"], bound, "pass" if s["hot_s"] <= bound else "miss")
    return out


def print_table(rows, arm=""):
    for r in rows:
        value = r["value"]
        if isinstance(value, float):
            value = "%.3f" % value
        bound = r["bound"]
        if isinstance(bound, float):
            bound = "%.3f" % bound
        line = "%-12s %-8s %-46s %-14s %-14s %s" % (
            r["verdict"].upper(), arm or r["kind"], r["band"], value, bound, r["note"])
        print(line.rstrip())


# --- request bodies --------------------------------------------------------

def request_bodies(corpus, out_dir):
    doc = json.load(open(corpus))
    entries = doc["entries"]
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "index.tsv"), "w") as idx:
        for n, e in enumerate(entries, 1):
            with open(os.path.join(out_dir, "%02d.json" % n), "w") as f:
                json.dump({"query": e["sql"]}, f)
            idx.write("%02d\t%s\n" % (n, e["id"]))
    print(len(entries))


# --- results.json ----------------------------------------------------------

def parse_settings(log_text):
    """The five `performance default resolved` stamps, in the shape
    report.rs's ServerSetting serialises to."""
    ansi = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
    settings = [{"setting": s, "values": [], "unreadable_lines": []} for s in STAMPED_SETTINGS]
    for raw in log_text.splitlines():
        line = ansi.sub("", raw)
        if " performance default resolved " not in line:
            continue
        m = re.search(r' setting="?([A-Za-z0-9_]+)"?', line)
        if not m:
            continue
        for entry in settings:
            if entry["setting"] == m.group(1):
                v = re.search(r" value=([0-9]+)(?: |$)", line)
                if v:
                    entry["values"].append(int(v.group(1)))
                else:
                    entry["unreadable_lines"].append(line.strip())
    return settings


def assemble(args):
    """assemble KEY=VALUE...: build results.json from the files run.sh wrote.
    Required keys: out, calls (the benchmark phase's calls.jsonl), index,
    server_log, bands (lane:path[,lane:path]), provenance (a directory of
    stamp files, one per provenance key), arm, sha, binary, tenant, phases_json."""
    kv = dict(a.split("=", 1) for a in args)
    for key in ("out", "calls", "index", "server_log", "bands", "provenance",
                "arm", "sha", "binary", "tenant", "phases_json"):
        if key not in kv:
            raise Refusal("assemble: missing %s=" % key)
    ids = {}
    for line in open(kv["index"]):
        n, sid = line.rstrip("\n").split("\t")
        ids[int(n)] = sid
    tries = {}
    for n, line in enumerate(open(kv["calls"]), 1):
        try:
            call = json.loads(line)
            tries.setdefault(int(call["number"]), []).append(call)
        except (ValueError, KeyError, TypeError) as e:
            raise Refusal("assemble: %s:%d is not a call record: %s" % (kv["calls"], n, e))
    statements = []
    for n in sorted(ids):
        rows = tries.get(n, [])
        base = {"number": n, "id": ids[n], "cold_s": None, "hot_s": None, "verdict": None,
                "float_mismatches": [], "missing_rows": 0, "extra_rows": 0,
                "compare_error": "not compared: the kit runs no reference comparison",
                "error": None, "tries_s": []}
        bad = [r for r in rows if r.get("outcome") != "ok"]
        if len(rows) != 3 or bad:
            first = bad[0] if bad else None
            base["error"] = ("%s HTTP %s %s: %s" % (first.get("outcome"), first.get("http_status"),
                                                    first.get("refusal_class"), first.get("error"))
                             if first else "%d of 3 tries recorded" % len(rows))
            base["compare_error"] = None
        else:
            t = [float(r["duration_s"]) for r in sorted(rows, key=lambda r: int(r["try"]))]
            base["tries_s"] = t
            base["cold_s"] = t[0]
            base["hot_s"] = min(t[1], t[2])
        statements.append(base)
    failed = [s["number"] for s in statements if s["error"] is not None]
    answered = [s for s in statements if s["error"] is None]
    totals = {
        "cold_sum_s": sum(s["cold_s"] for s in answered),
        "hot_sum_s": sum(s["hot_s"] for s in answered),
        "answered": len(answered),
        "failed": failed,
    }
    lanes = []
    for item in kv["bands"].split(","):
        lane, _, path = item.partition(":")
        if not lane or not path:
            raise Refusal("assemble: bands= takes lane:path[,lane:path], got %r" % item)
        lanes.append((lane, path))
    allowed = set()
    for _, path in lanes:
        data, _ = check_bands(path)
        allowed |= set(int(str(x).lstrip("q")) for x in data.get("failures", []))
    settings = parse_settings(open(kv["server_log"], errors="replace").read())

    def single(name):
        for s in settings:
            if s["setting"] == name and len(s["values"]) == 1 and not s["unreadable_lines"]:
                return s["values"][0]
        raise Refusal("server log %s does not carry %s exactly once" % (kv["server_log"], name))

    provenance = {
        "git_sha": kv["sha"],
        "binary": kv["binary"],
        "arm": kv["arm"].lower(),
        "location": "tenant %s" % kv["tenant"],
        "server": kv.get("server", "http://127.0.0.1:4318"),
        "server_log": kv["server_log"],
        "server_settings": settings,
        "sql_max_query_bytes": single("sql_max_query_bytes"),
        "sql_tenant_max_bytes": single("sql_tenant_max_bytes"),
        "reference_dir": "",
        "reference_version": "none: the kit compares no answers",
        "ddl_files": None,
    }
    # One file per stamp: a file that parses as JSON is that value, any
    # other file is its text with the trailing newline dropped.
    for name in sorted(os.listdir(kv["provenance"])):
        text = open(os.path.join(kv["provenance"], name)).read()
        try:
            provenance[name] = json.loads(text)
        except ValueError:
            provenance[name] = text.rstrip("\n")
    report = {
        "provenance": provenance,
        "statements": statements,
        "totals": totals,
        "registered_failures_answered": [s["number"] for s in answered if s["number"] in allowed],
        "concurrency": None,
        "concurrency_error": None,
        "phases": json.load(open(kv["phases_json"])),
    }
    report["bands"] = []
    for lane, path in lanes:
        for row in absolute_bands(path, report):
            row["lane"] = lane
            report["bands"].append(row)
    tmp = kv["out"] + ".tmp"
    with open(tmp, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)
        f.write("\n")
    os.rename(tmp, kv["out"])
    validate(kv["out"])
    print_table(report["bands"], kv["arm"])


def phases_summary(out_dir, phases):
    """Per-phase call and error counts from each out/<phase>/calls.jsonl."""
    summary = {}
    for phase in phases:
        path = os.path.join(out_dir, phase, "calls.jsonl")
        calls = errors = 0
        classes = {}
        if os.path.exists(path):
            for line in open(path):
                call = json.loads(line)
                calls += 1
                if call.get("outcome") != "ok":
                    errors += 1
                    cls = call.get("refusal_class") or call.get("outcome")
                    classes[cls] = classes.get(cls, 0) + 1
        summary[phase] = {"calls": calls, "errors": errors, "refusal_classes": classes,
                          "out_dir": "out/%s/" % phase}
    print(json.dumps(summary, indent=2, sort_keys=True))


def validate(path):
    try:
        r = json.load(open(path))
    except (OSError, ValueError) as e:
        raise Refusal("%s: not readable JSON: %s" % (path, e))
    if not isinstance(r, dict):
        raise Refusal("%s: not a JSON object" % path)
    missing = [k for k in REPORT_FIELDS + ("bands", "phases") if k not in r]
    if missing:
        raise Refusal("%s: missing %s" % (path, ", ".join(missing)))
    p = r["provenance"]
    missing = [k for k in PROVENANCE_FIELDS + KIT_STAMPS if k not in p]
    if missing:
        raise Refusal("%s: provenance missing %s" % (path, ", ".join(missing)))
    if p["arm"] not in ("a", "b") or p["kit_arm"] not in ("A", "B"):
        raise Refusal("%s: provenance arm %r / kit_arm %r" % (path, p["arm"], p["kit_arm"]))
    if not SHA_RE.match(str(p["git_sha"])):
        raise Refusal("%s: provenance git_sha %r is not a full commit" % (path, p["git_sha"]))
    for k in ("start", "end"):
        if k not in (p["burst_balance"] or {}):
            raise Refusal("%s: provenance burst_balance has no %s" % (path, k))
    if not isinstance(r["statements"], list) or not r["statements"]:
        raise Refusal("%s: no statements" % path)
    for s in r["statements"]:
        miss = [k for k in STATEMENT_FIELDS if k not in s]
        if miss:
            raise Refusal("%s: statement %r missing %s" % (path, s.get("number"), ", ".join(miss)))
    miss = [k for k in TOTALS_FIELDS if k not in r["totals"]]
    if miss:
        raise Refusal("%s: totals missing %s" % (path, ", ".join(miss)))
    for b in r["bands"]:
        if b.get("verdict") not in ("pass", "miss", "inconclusive"):
            raise Refusal("%s: band %r has verdict %r" % (path, b.get("band"), b.get("verdict")))
    return r


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 64
    cmd, args = argv[1], argv[2:]
    try:
        if cmd == "get" and len(args) == 2:
            data, _ = parse_toml(args[0])
            v = lookup(data, args[1])
            if v is None:
                raise Refusal("%s: no key %s" % (args[0], args[1]))
            for item in (v if isinstance(v, list) else [v]):
                print(item if not isinstance(item, bool) else str(item).lower())
        elif cmd == "check-run" and len(args) == 1:
            check_run(args[0])
        elif cmd == "check-bands" and len(args) == 1:
            check_bands(args[0])
        elif cmd == "request-bodies" and len(args) == 2:
            request_bodies(args[0], args[1])
        elif cmd == "phases" and len(args) >= 2:
            phases_summary(args[0], args[1:])
        elif cmd == "assemble":
            assemble(args)
        elif cmd == "validate" and len(args) == 1:
            validate(args[0])
        elif cmd == "absolute" and len(args) == 2:
            rows = validate(args[1])["bands"]
            print_table(rows, args[0])
            return 1 if any(r["verdict"] == "miss" for r in rows) else 0
        elif cmd == "relative" and len(args) == 3:
            a, b = validate(args[1]), validate(args[2])
            rows = relative_bands(args[0], a, b)
            print_table(rows, "B vs A")
            return 1 if any(r["verdict"] == "miss" for r in rows) else 0
        else:
            print(__doc__, file=sys.stderr)
            return 64
    except Refusal as e:
        print("kit: %s" % e, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
