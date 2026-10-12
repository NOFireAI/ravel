#!/usr/bin/env python3
"""Shared helpers for the ClickBench measurement kit (ADR-2867 change 2).

launch.sh and fetch.sh run this on the laptop and box/run.sh runs it on the
box, so it uses the standard library only and runs on Python 3.6 (no
tomllib): both macOS's python3 and Amazon Linux 2023's are older than 3.11.

Subcommands (each exits 0 on success, 2 on a refusal or an unexpected error,
64 on bad usage):

  get FILE KEY                 print one run.toml value; an array prints one
                               item per line; a dotted key reads a table
  check-run FILE               refuse a run.toml missing a required key,
                               holding a key or table it does not know
                               (named), or holding a malformed SHA, lane,
                               phase, band section or digest
  check-bands LANE FILE [SECTION...]
                               refuse a band file that does not match the
                               lane's schema (a missing, unknown or removed
                               key, named) or that holds a placeholder value
                               in a section it will be judged by (default:
                               every section)
  request-bodies CORPUS DIR START END
                               write DIR/NN.json ({"query", "start", "end"})
                               and DIR/index.tsv (NN, statement id, expected
                               row count or -) per statement
  phases OUT PHASE...          print per-phase call and error counts from
                               OUT/<phase>/calls.jsonl as JSON
  assemble ARGS...             write results.json from the box's phase output
  validate FILE                refuse a results.json missing a field the
                               shape below requires
  absolute LABEL FILE          print a results.json's absolute band table,
                               exit 1 on a miss
  relative BANDS A.json B.json refuse two arms that are not comparable, else
                               evaluate the lane's relative bands, print the
                               table, exit 1 on a miss

results.json carries SqlLatencyReport's statement shape
(crates/ravel-bench/src/sql_latency.rs), so the runbook's analyse.py
(docs/internal/clickbench-aws-runbook.md section 9) reads it unchanged:
`entries` holds one EntryReport per answered statement (id, min_ms, median_ms,
max_ms, cold_ms, rows_returned, per_run_accounting) and `failed` one
FailedEntry per statement that did not answer (id, run, error). Both carry the
kit's `answer` check and entries carry `"error": null`. per_run_accounting is
null: an HTTP answer carries no executor accounting, as on the Flight lane.
min_ms (hot) is the best of tries 2 and 3 and cold_ms is try 1; median_ms and
max_ms run over all three tries. The report says so in its own
timing_definition string, because SqlLatencyReport's min_ms runs over all
three. The kit's own blocks sit beside them: provenance, totals, error_pooled,
concurrency, phases and bands.
"""

import json
import math
import os
import re
import sys
import traceback

PLACEHOLDER_PREFIX = "FILL-IN"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")

# Phases box/run.sh implements. T5 adds its phases here and in run.sh.
KNOWN_PHASES = ("benchmark",)

# Band file sections a run.toml can select. `absolute` covers the [absolute]
# table and every [absolute.*] table under it.
BAND_SECTIONS = ("relative", "absolute")

STATEMENT_KEYS = tuple("q%02d" % n for n in range(1, 44))

# Each lane the kit runs, by table: the keys its band file must hold and the
# only ones it may hold. The parquet lane's bands/parquet.toml belongs to
# clickbench_parquet_bench, which runs no kit phase yet.
LANE_BANDS = {
    "rlog": {
        "relative": (
            "error_pooled_max_delta", "qps_min_ratio", "hot_statement_max_ratio",
            "hot_statement_floor_s", "hot_sum_max_ratio", "cold_sum_max_ratio",
            "min_both_answered",
        ),
        "absolute": ("hot_sum_s_max", "cold_sum_s_max", "error_pooled_max"),
        "absolute.hot_s_max": STATEMENT_KEYS,
    },
}
# Keys a band file may no longer carry, refused by name (parse_prereg in
# crates/ravel-bench/src/clickbench_parquet/report.rs refuses the same).
REMOVED_BAND_KEYS = ("concurrency_error_ratio_ceiling",)
# Band keys that count statements: a whole number from 1 to the corpus size.
COUNT_BAND_KEYS = ("relative.min_both_answered",)

RUN_REQUIRED = (
    "sha_a", "sha_b", "lanes", "phases", "instance_type", "region", "bucket",
    "prefix", "instance_profile", "repo_url", "tenant", "shards", "corpus",
    "dataset_url", "dataset_sha256", "rows_expected", "rustfs_url",
    "rustfs_sha256", "memory_budget_bytes", "audit_mode",
)
# Read by box/run.sh (band_sections through query_timeout_s) or launch.sh
# (band_sections, ami through key_name) when present.
RUN_OPTIONAL = (
    "band_sections", "window_hours", "audit_text", "expected_statements",
    "fold_min_entry_ratio", "query_timeout_s", "ami", "subnet",
    "security_group", "key_name",
)
# run.toml's only table.
RUN_TABLES = ("bands",)
DEFAULT_WINDOW_HOURS = 200000

ENTRY_FIELDS = (
    "id", "min_ms", "median_ms", "max_ms", "cold_ms", "rows_returned",
    "per_run_accounting", "error", "answer",
)
FAILED_FIELDS = ("id", "run", "error", "answer")
ANSWER_FIELDS = ("answered", "rows", "expected_rows", "match")
REPORT_FIELDS = (
    "provenance", "timing_definition", "entries", "skipped", "failed", "totals",
    "error_pooled", "concurrency", "phases", "bands",
)
TIMING_DEFINITION = ("hot (min_ms) is the best of tries 2 and 3; cold (cold_ms) is try 1; "
                     "median_ms and max_ms are over all three tries")
PROVENANCE_FIELDS = (
    "git_sha", "binary", "arm", "tenant", "server", "server_logs",
    "server_settings", "band_sections",
)
TOTALS_FIELDS = ("cold_sum_s", "hot_sum_s", "answered", "failed")
# The provenance stamps run.sh writes, one file each.
KIT_STAMPS = (
    "kit_arm", "run_id", "lanes", "toolchain", "hardware", "backend",
    "corpus_digest", "server_flags", "audit_mode", "burst_balance", "fold",
    "object_counts", "query_window", "row_count",
)
STAMPED_SETTINGS = (
    "cache_max_bytes", "catalog_cache_max_bytes", "fetch_concurrency",
    "sql_max_query_bytes", "sql_tenant_max_bytes",
)
# Two arms whose provenance differs on one of these did not measure the same
# thing, so no relative band between them stands. server_settings is compared
# whole: it holds only the resolved STAMPED_SETTINGS, and each arm's binary
# path and SHA live in provenance.binary, git_sha and server_flags, which are
# not compared because they are what differs between the arms by design.
COMPARABLE = (
    "corpus_digest.dataset_sha256",
    "corpus_digest.mapping_sha256",
    "corpus_digest.corpus_sha256",
    "hardware.cpu",
    "backend.version",
    "audit_mode",
    "query_window.window_hours",
    "server_settings",
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
    `key = value` with strings, numbers, booleans and one-line arrays. A
    dotted table name ([a.b]) is kept as one flat key "a.b"."""
    data, table = {}, None
    try:
        lines = open(path).read().splitlines()
    except OSError as e:
        raise Refusal("cannot read %s: %s" % (path, e))
    for n, raw in enumerate(lines, 1):
        where = "%s:%d" % (path, n)
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
    return data


def lookup(data, key):
    node = data
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


# --- run.toml --------------------------------------------------------------

def band_sections(data):
    sections = data.get("band_sections", list(BAND_SECTIONS))
    if not isinstance(sections, list) or not sections:
        raise Refusal("band_sections must be a non-empty array")
    for s in sections:
        if s not in BAND_SECTIONS:
            raise Refusal("band_sections holds %r; known sections: %s"
                          % (s, ", ".join(BAND_SECTIONS)))
    return sections


def check_run(path):
    data = parse_toml(path)
    tables = sorted(k for k, v in data.items() if isinstance(v, dict))
    unknown = sorted(k for k, v in data.items() if not isinstance(v, dict)
                     and k not in RUN_REQUIRED and k not in RUN_OPTIONAL)
    if unknown:
        raise Refusal("run.toml %s: unknown key(s) %s" % (path, ", ".join(unknown)))
    unknown = [t for t in tables if t not in RUN_TABLES]
    if unknown:
        raise Refusal("run.toml %s: unknown table(s) %s"
                      % (path, ", ".join("[%s]" % t for t in unknown)))
    unknown = sorted("bands." + k for k in data.get("bands", {}) if k not in LANE_BANDS)
    if unknown:
        raise Refusal("run.toml %s: unknown key(s) %s; [bands] names one band file per lane (%s)"
                      % (path, ", ".join(unknown), ", ".join(sorted(LANE_BANDS))))
    missing =[k for k in RUN_REQUIRED if lookup(data, k) in (None, "")]
    if missing:
        raise Refusal("run.toml %s: missing required key(s): %s" % (path, ", ".join(missing)))
    for key in ("sha_a", "sha_b"):
        if not SHA_RE.match(str(data[key])):
            raise Refusal("run.toml %s: %s must be a full 40-character commit, got %r"
                          % (path, key, data[key]))
    if not SHA256_RE.match(str(data["dataset_sha256"])):
        raise Refusal("run.toml %s: dataset_sha256 must be 64 lowercase hex digits, got %r"
                      % (path, data["dataset_sha256"]))
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
    for lane in data["lanes"]:
        if lane not in LANE_BANDS:
            raise Refusal("run.toml %s: lane %r is not one the kit runs (%s)"
                          % (path, lane, ", ".join(sorted(LANE_BANDS))))
    bands = data.get("bands")
    for lane in data["lanes"]:
        if not isinstance(bands, dict) or not isinstance(bands.get(lane), str):
            raise Refusal("run.toml %s: lane %r has no band file under [bands]" % (path, lane))
    try:
        band_sections(data)
    except Refusal as e:
        raise Refusal("run.toml %s: %s" % (path, e))
    for key in ("shards", "memory_budget_bytes", "rows_expected"):
        if not isinstance(data[key], int) or isinstance(data[key], bool) or data[key] <= 0:
            raise Refusal("run.toml %s: %s must be a whole number above 0" % (path, key))
    hours = data.get("window_hours", DEFAULT_WINDOW_HOURS)
    if not isinstance(hours, int) or isinstance(hours, bool) or hours <= 0:
        raise Refusal("run.toml %s: window_hours must be a whole number above 0" % path)
    if data["audit_mode"] not in ("off", "required"):
        raise Refusal("run.toml %s: audit_mode must be off or required" % path)
    if data.get("audit_text", "redacted") not in ("redacted", "plaintext"):
        raise Refusal("run.toml %s: audit_text must be redacted or plaintext" % path)
    if data.get("audit_text") == "plaintext" and data["audit_mode"] != "required":
        raise Refusal("run.toml %s: audit_text plaintext needs audit_mode required "
                      "(the server refuses it otherwise)" % path)
    return data


# --- band files ------------------------------------------------------------

def _num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _placeholder(v):
    return isinstance(v, str) and v.startswith(PLACEHOLDER_PREFIX)


def _section_of(table):
    return table.split(".", 1)[0]


def check_bands(lane, path, sections=None):
    """The lane's band file, checked the way parse_prereg checks a
    pre-registration: removed keys, then unknown keys, then missing ones, each
    named. A placeholder is refused only in a section the run selected; any
    other value must be a number."""
    if lane not in LANE_BANDS:
        raise Refusal("band file %s: lane %r is not one the kit runs (%s)"
                      % (path, lane, ", ".join(sorted(LANE_BANDS))))
    schema = LANE_BANDS[lane]
    sections = list(BAND_SECTIONS) if sections is None else sections
    data = parse_toml(path)
    removed, unknown = [], []
    for table, node in sorted(data.items()):
        if not isinstance(node, dict):
            (removed if table in REMOVED_BAND_KEYS else unknown).append(table)
            continue
        for key in sorted(node):
            name = "%s.%s" % (table, key)
            if key in REMOVED_BAND_KEYS:
                removed.append(name)
            elif table not in schema or key not in schema[table]:
                unknown.append(name)
    if removed:
        raise Refusal("band file %s: removed key(s) %s; delete them"
                      % (path, ", ".join(removed)))
    if unknown:
        raise Refusal("band file %s: unknown key(s) %s for lane %s"
                      % (path, ", ".join(unknown), lane))
    missing = ["%s.%s" % (t, k) for t in sorted(schema) for k in schema[t]
               if k not in data.get(t, {})]
    if missing:
        raise Refusal("band file %s: missing required key(s) %s" % (path, ", ".join(missing)))
    placeholders, malformed = [], []
    for table in sorted(schema):
        for key in schema[table]:
            v, name = data[table][key], "%s.%s" % (table, key)
            if _placeholder(v):
                if _section_of(table) in sections:
                    placeholders.append(name)
            elif not _num(v) or v < 0:
                malformed.append(name)
    if malformed:
        raise Refusal("band file %s: %s must be a number of at least 0"
                      % (path, ", ".join(malformed)))
    for name in COUNT_BAND_KEYS:
        table, key = name.rsplit(".", 1)
        if key not in schema.get(table, ()):
            continue
        v = data[table][key]
        if not _placeholder(v) and (not isinstance(v, int) or not 1 <= v <= len(STATEMENT_KEYS)):
            raise Refusal("band file %s: %s must be a whole number from 1 to %d, got %r"
                          % (path, name, len(STATEMENT_KEYS), v))
    if placeholders:
        raise Refusal("band file %s: placeholder value in %s; fill it in on the reference "
                      "machine before selecting that section in band_sections"
                      % (path, ", ".join(placeholders)))
    return data


def lane_of(path):
    return os.path.splitext(os.path.basename(path))[0]


def _row(kind, band, value, bound, verdict, note="", statement=None):
    row = {"band": band, "kind": kind, "value": value, "bound": bound,
           "verdict": verdict, "note": note}
    if statement is not None:
        row["statement"] = statement
    return row


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


def absolute_bands(lane, path, sections, report):
    """One arm judged against the lane's absolute section, when selected."""
    if "absolute" not in sections:
        return []
    data = check_bands(lane, path, sections)
    ab, per = data["absolute"], data["absolute.hot_s_max"]
    m = run_metrics(report)
    out = []
    for metric in ("hot_sum_s", "cold_sum_s", "error_pooled"):
        bound, value = ab[metric + "_max"], m[metric]
        label = "%s <= %s_max" % (metric, metric)
        if not _num(value):
            out.append(_row("absolute", label, None, bound, "miss", "not measured"))
        else:
            out.append(_row("absolute", label, value, bound, "pass" if value <= bound else "miss"))
    answered = {e["id"]: e for e in report["entries"]}
    for sid in sorted(set(answered) | set(f["id"] for f in report["failed"])):
        q = sid.split("_", 1)[0]
        if q not in per:
            raise Refusal("band file %s: no absolute.hot_s_max key for statement %s" % (path, sid))
        label = "%s hot_s <= hot_s_max.%s" % (q, q)
        if sid not in answered:
            out.append(_row("absolute", label, None, per[q], "miss", "not answered", sid))
            continue
        hot = answered[sid]["min_ms"] / 1000.0
        out.append(_row("absolute", label, hot, per[q], "pass" if hot <= per[q] else "miss",
                        "", sid))
    return out


def check_comparable(a, b):
    pa, pb = a["provenance"], b["provenance"]
    for field in COMPARABLE:
        va, vb = lookup(pa, field), lookup(pb, field)
        if va in (None, "") or vb in (None, ""):
            raise Refusal("arms are not comparable: provenance.%s is missing on %s"
                          % (field, "A" if va in (None, "") else "B"))
        if va != vb:
            raise Refusal("arms are not comparable: provenance.%s differs (A %r, B %r)"
                          % (field, va, vb))
    if pa["band_sections"] != pb["band_sections"]:
        raise Refusal("arms are not comparable: provenance.band_sections differs (A %r, B %r)"
                      % (pa["band_sections"], pb["band_sections"]))


def _qs(ids):
    return " ".join(sid.split("_", 1)[0] for sid in sorted(ids))


def relative_bands(path, a, b):
    """Arm B judged against arm A, over the statements both arms answered. A
    statement A answered and B did not is a miss of its own, and so is a
    both-answered set smaller than min_both_answered, an arm that answered
    nothing, or a statement whose row count differs between the arms."""
    check_comparable(a, b)
    sections = a["provenance"]["band_sections"]
    if "relative" not in sections:
        return []
    rel = check_bands(lane_of(path), path, sections)["relative"]
    out = []

    def add(*args, **kw):
        out.append(_row("relative", *args, **kw))

    ma, mb = run_metrics(a), run_metrics(b)
    d = rel["error_pooled_max_delta"]
    if _num(ma["error_pooled"]) and _num(mb["error_pooled"]):
        bound = ma["error_pooled"] + d
        add("error_pooled <= A + %g" % d, mb["error_pooled"], bound,
            "pass" if mb["error_pooled"] <= bound else "miss")
    else:
        add("error_pooled <= A + %g" % d, None, None, "inconclusive", "no calls recorded")
    r = rel["qps_min_ratio"]
    if _num(ma["qps"]) and _num(mb["qps"]):
        bound = r * ma["qps"]
        add("qps >= %g x A" % r, mb["qps"], bound, "pass" if mb["qps"] >= bound else "miss")
    else:
        add("qps >= %g x A" % r, None, None, "inconclusive", "no concurrency phase ran")

    ea = {e["id"]: e for e in a["entries"]}
    eb = {e["id"]: e for e in b["entries"]}
    both = sorted(set(ea) & set(eb))
    ids = set(ea) | set(eb) | set(f["id"] for r in (a, b) for f in r["failed"])
    need = rel["min_both_answered"]
    notes = ["not answered by %s: %s" % (arm, _qs(ids - set(e)))
             for arm, e in (("A", ea), ("B", eb)) if ids - set(e)]
    notes += ["arm %s answered no statement" % arm for arm, e in (("A", ea), ("B", eb)) if not e]
    add("both answered >= %d" % need, len(both), need,
        "pass" if len(both) >= need and ea and eb else "miss", "; ".join(notes))
    for sid in sorted(set(ea) - set(eb)):
        add("%s answered by B" % sid.split("_", 1)[0], None, None, "miss",
            "answered by A, not by B", statement=sid)
    differ = [s for s in both if ea[s]["rows_returned"] != eb[s]["rows_returned"]]
    add("rows_returned = A (%d both answered)" % len(both), len(differ), 0,
        "miss" if differ else "pass", _qs(differ))
    for metric, field, key in (("hot_sum_s", "min_ms", "hot_sum_max_ratio"),
                               ("cold_sum_s", "cold_ms", "cold_sum_max_ratio")):
        sa = sum(ea[s][field] for s in both) / 1000.0
        sb = sum(eb[s][field] for s in both) / 1000.0
        bound = rel[key] * sa
        add("%s <= %g x A (%d both answered)" % (metric, rel[key], len(both)), sb, bound,
            "pass" if sb <= bound else "miss")
    r = rel["hot_statement_max_ratio"]
    floor = rel["hot_statement_floor_s"]
    for sid in sorted(set(ea) | set(eb)):
        label = "%s hot_s <= max(%g x A, %g s)" % (sid.split("_", 1)[0], r, floor)
        if sid not in both:
            if sid in eb:
                add(label, eb[sid]["min_ms"] / 1000.0, None, "inconclusive", "failed on A",
                    statement=sid)
            continue
        ah, bh = ea[sid]["min_ms"] / 1000.0, eb[sid]["min_ms"] / 1000.0
        bound = max(r * ah, floor)
        add(label, bh, bound, "pass" if bh <= bound else "miss", statement=sid)
    for sid in differ:
        add("%s rows_returned = A" % sid.split("_", 1)[0], eb[sid]["rows_returned"],
            ea[sid]["rows_returned"], "miss", statement=sid)
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

def request_bodies(corpus, out_dir, start, end):
    """One body per statement with an explicit window: the server's default
    is the last hour, which would hide every commit the load made earlier."""
    try:
        start, end = int(start), int(end)
    except ValueError:
        raise Refusal("request-bodies: START and END must be whole Unix seconds")
    if start >= end:
        raise Refusal("request-bodies: START %d is not before END %d" % (start, end))
    doc = json.load(open(corpus))
    entries = doc["entries"]
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "index.tsv"), "w") as idx:
        for n, e in enumerate(entries, 1):
            with open(os.path.join(out_dir, "%02d.json" % n), "w") as f:
                json.dump({"query": e["sql"], "start": start, "end": end}, f)
            expected = e.get("expected_rows")
            idx.write("%02d\t%s\t%s\n" % (n, e["id"], "-" if expected is None else int(expected)))
    print(len(entries))


# --- results.json ----------------------------------------------------------

def parse_settings(log_text):
    """Every `performance default resolved` value per stamped setting."""
    ansi = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
    settings = dict((s, []) for s in STAMPED_SETTINGS)
    unreadable = []
    for raw in log_text.splitlines():
        line = ansi.sub("", raw)
        if " performance default resolved " not in line:
            continue
        m = re.search(r' setting="?([A-Za-z0-9_]+)"?', line)
        if not m or m.group(1) not in settings:
            continue
        v = re.search(r" value=([0-9]+)(?: |$)", line)
        if v:
            settings[m.group(1)].append(int(v.group(1)))
        else:
            unreadable.append(line.strip())
    return settings, unreadable


def server_settings(logs):
    """The settings every benchmark server start resolved. Each restart must
    resolve the same values, and the two memory caps exactly once, or the
    statements did not all run under one configuration."""
    first, first_log = None, None
    for log in logs:
        settings, unreadable = parse_settings(open(log, errors="replace").read())
        if unreadable:
            raise Refusal("server log %s: unreadable setting line %r" % (log, unreadable[0]))
        for name in ("sql_max_query_bytes", "sql_tenant_max_bytes"):
            if len(settings[name]) != 1:
                raise Refusal("server log %s carries %s %d times, not once"
                              % (log, name, len(settings[name])))
        if first is None:
            first, first_log = settings, log
            continue
        for name in STAMPED_SETTINGS:
            if settings[name] != first[name]:
                raise Refusal("server log %s resolved %s %r, %s resolved %r"
                              % (log, name, settings[name], first_log, first[name]))
    if first is None:
        raise Refusal("assemble: no server log")
    return dict((k, v[0] if len(v) == 1 else v) for k, v in first.items())


def _median(xs):
    s = sorted(xs)
    return s[(len(s) - 1) // 2]


def statement_report(sid, expected, calls):
    """EntryReport for an answered statement, FailedEntry otherwise."""
    calls = sorted(calls, key=lambda c: int(c["try"]))
    rows = [c.get("rows") for c in calls if isinstance(c.get("rows"), int)]
    answer = {"answered": False, "rows": rows[0] if rows else None,
              "expected_rows": expected, "match": None}
    if expected is not None:
        answer["match"] = bool(rows) and all(r == expected for r in rows)
    bad = [c for c in calls if c.get("outcome") != "ok"]
    if len(calls) != 3 or bad:
        first = bad[0] if bad else None
        if first:
            error = "%s HTTP %s %s: %s" % (first.get("outcome"), first.get("http_status"),
                                          first.get("refusal_class"), first.get("error"))
            run = int(first["try"]) - 1
        else:
            error, run = "%d of 3 tries recorded" % len(calls), len(calls)
        return None, {"id": sid, "run": run, "error": error, "answer": answer}
    answer["answered"] = True
    ms = [float(c["duration_s"]) * 1000.0 for c in calls]
    return {"id": sid, "min_ms": min(ms[1:]), "median_ms": _median(ms), "max_ms": max(ms),
            "cold_ms": ms[0], "rows_returned": answer["rows"], "per_run_accounting": None,
            "error": None, "answer": answer, "tries_ms": ms}, None


def assemble(args):
    """assemble KEY=VALUE...: build results.json from the files run.sh wrote.
    Required keys: out, calls (the benchmark phase's calls.jsonl), index,
    server_logs (comma-separated, every benchmark server start), bands
    (lane:path[,lane:path]), sections (comma-separated band sections),
    provenance (a directory of stamp files, one per provenance key), arm, sha,
    binary, tenant, phases_json."""
    kv = dict(a.split("=", 1) for a in args)
    for key in ("out", "calls", "index", "server_logs", "bands", "sections", "provenance",
                "arm", "sha", "binary", "tenant", "phases_json"):
        if key not in kv:
            raise Refusal("assemble: missing %s=" % key)
    sections = band_sections({"band_sections": kv["sections"].split(",")})
    index = []
    for line in open(kv["index"]):
        n, sid, expected = line.rstrip("\n").split("\t")
        index.append((int(n), sid, None if expected == "-" else int(expected)))
    tries = {}
    for n, line in enumerate(open(kv["calls"]), 1):
        try:
            call = json.loads(line)
            tries.setdefault(int(call["number"]), []).append(call)
        except (ValueError, KeyError, TypeError) as e:
            raise Refusal("assemble: %s:%d is not a call record: %s" % (kv["calls"], n, e))
    entries, failed = [], []
    for n, sid, expected in index:
        entry, fail = statement_report(sid, expected, tries.get(n, []))
        (entries if entry else failed).append(entry or fail)
    totals = {
        "cold_sum_s": sum(e["cold_ms"] for e in entries) / 1000.0,
        "hot_sum_s": sum(e["min_ms"] for e in entries) / 1000.0,
        "answered": len(entries),
        "failed": [f["id"] for f in failed],
    }
    lanes = []
    for item in kv["bands"].split(","):
        lane, _, path = item.partition(":")
        if not lane or not path:
            raise Refusal("assemble: bands= takes lane:path[,lane:path], got %r" % item)
        lanes.append((lane, path))
    logs = kv["server_logs"].split(",")
    provenance = {
        "git_sha": kv["sha"],
        "binary": kv["binary"],
        "arm": kv["arm"].lower(),
        "tenant": kv["tenant"],
        "server": kv.get("server", "http://127.0.0.1:4318"),
        "server_logs": [os.path.basename(p) for p in logs],
        "server_settings": server_settings(logs),
        "band_sections": sections,
    }
    # One file per stamp: a file that parses as JSON is that value, any
    # other file is its text with the trailing newline dropped.
    for name in sorted(os.listdir(kv["provenance"])):
        text = open(os.path.join(kv["provenance"], name)).read()
        try:
            provenance[name] = json.loads(text)
        except ValueError:
            provenance[name] = text.rstrip("\n")
    phases = json.load(open(kv["phases_json"]))
    calls = sum(p.get("calls", 0) for p in phases.values())
    errors = sum(p.get("errors", 0) for p in phases.values())
    report = {
        "provenance": provenance,
        "timing_definition": TIMING_DEFINITION,
        "entries": entries,
        "skipped": [],
        "failed": failed,
        "totals": totals,
        "error_pooled": {"calls": calls, "errors": errors,
                         "ratio": (errors / calls) if calls else None},
        "concurrency": None,
        "phases": phases,
    }
    report["bands"] = []
    for lane, path in lanes:
        for row in absolute_bands(lane, path, sections, report):
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
    missing = [k for k in REPORT_FIELDS if k not in r]
    if missing:
        raise Refusal("%s: missing %s" % (path, ", ".join(missing)))
    if r["timing_definition"] != TIMING_DEFINITION:
        raise Refusal("%s: timing_definition %r is not this kit's (%r)"
                      % (path, r["timing_definition"], TIMING_DEFINITION))
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
    for k in ("start", "end", "window_hours"):
        if not _num((p["query_window"] or {}).get(k)):
            raise Refusal("%s: provenance query_window has no %s" % (path, k))
    counts = [(p["row_count"] or {}).get(k) for k in ("rows_expected", "load_rows_written",
                                                      "count_star")]
    if not all(isinstance(c, int) and not isinstance(c, bool) for c in counts) \
            or len(set(counts)) != 1:
        raise Refusal("%s: provenance row_count %r does not hold one row count"
                      % (path, p["row_count"]))
    seen = set()
    for kind, fields in (("entries", ENTRY_FIELDS), ("failed", FAILED_FIELDS)):
        for e in r[kind]:
            miss = [k for k in fields if k not in e]
            if miss:
                raise Refusal("%s: %s row %r missing %s" % (path, kind, e.get("id"), ", ".join(miss)))
            miss = [k for k in ANSWER_FIELDS if k not in (e["answer"] or {})]
            if miss:
                raise Refusal("%s: %s row %r answer missing %s"
                              % (path, kind, e["id"], ", ".join(miss)))
            if e["id"] in seen:
                raise Refusal("%s: statement %s appears twice" % (path, e["id"]))
            seen.add(e["id"])
    if not seen:
        raise Refusal("%s: no statements" % path)
    for e in r["entries"]:
        for k in ("min_ms", "median_ms", "max_ms", "cold_ms"):
            if not _num(e[k]):
                raise Refusal("%s: entry %s has %s %r" % (path, e["id"], k, e[k]))
    miss = [k for k in TOTALS_FIELDS if k not in r["totals"]]
    if miss:
        raise Refusal("%s: totals missing %s" % (path, ", ".join(miss)))
    for k in ("cold_sum_s", "hot_sum_s"):
        if not _num(r["totals"][k]):
            raise Refusal("%s: totals.%s is %r, not a number" % (path, k, r["totals"][k]))
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
            v = lookup(parse_toml(args[0]), args[1])
            if v is None:
                raise Refusal("%s: no key %s" % (args[0], args[1]))
            for item in (v if isinstance(v, list) else [v]):
                print(item if not isinstance(item, bool) else str(item).lower())
        elif cmd == "check-run" and len(args) == 1:
            check_run(args[0])
        elif cmd == "check-bands" and len(args) >= 2:
            check_bands(args[0], args[1], args[2:] or None)
        elif cmd == "request-bodies" and len(args) == 4:
            request_bodies(*args)
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
    except Exception:
        # Exit 1 means a band miss to every caller, so a crash must not read
        # as one.
        traceback.print_exc()
        print("kit: unexpected error in %s; see the traceback above" % cmd, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
