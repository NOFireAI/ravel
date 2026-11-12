#!/usr/bin/env python3
"""Shared helpers for the ClickBench measurement kit (ADR-2867 change 2).

launch.sh and fetch.sh run this on the laptop and box/run.sh runs it on the
box, so it uses the standard library only and runs on Python 3.6 (no
tomllib): both macOS's python3 and Amazon Linux 2023's are older than 3.11.

Subcommands (each exits 0 on success, 2 on a refusal or an unexpected error,
64 on bad usage):

  get FILE KEY                 print one run.toml value; an array prints one
                               item per line; a dotted key reads a table;
                               exits 3 when FILE parses and lacks KEY
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
  resolved-run FILE OUT        write FILE to OUT with each phase key FILE
                               leaves out set to its default
  phases OUT PHASE...          print per-phase call and error counts from
                               OUT/<phase>/calls.jsonl as JSON
  permutation SEED CONN NQ     print connection CONN's statement order, as
                               upstream bench_concurrent_qps draws it
  concurrent-answers DIR       settle the deferred calls in DIR's
                               conn-*.jsonl from their response files: rows,
                               outcome, refusal class (wrong_answer among
                               them) and error, as run_query would have
  concurrent-summary DIR       print the concurrent block from DIR's
                               rounds.jsonl and calls.jsonl as JSON
  q29-summary DIR              print the q29_probe block from DIR/calls.jsonl
  fold-perf                    fold `perf script` output on stdin into one
                               `stack count` line per distinct stack
  concurrent-detail BANDS_DIR A.json A.tgz B.json B.tgz [RUN_TOML]
                               print each arm's per-round and pooled concurrent
                               figures and, on a missed concurrent error band,
                               that arm's errors by statement from its
                               evidence tarball
  healthz-sampler DIR GATE ACK BASE_4318 BASE_4319
                               sample /healthz on both listeners every
                               250 ms and the heartbeat age from
                               BASE_4318/metrics every 1 s into DIR, until
                               killed
  healthz-summary PHASE DIR    print PHASE's HEALTHZ line per port and its
                               HEARTBEAT line (scrapes, unanswered scrapes,
                               answered but unusable scrapes, max age) from
                               DIR's gated samples
  healthz-check FILE PHASE...  refuse FILE unless it holds each PHASE's
                               HEALTHZ and HEARTBEAT lines exactly once, else
                               print the results.json healthz block
  assemble ARGS...             write results.json from the box's phase output
  validate FILE                refuse a results.json missing a field the
                               shape below requires
  absolute LABEL FILE          print a results.json's absolute band table,
                               exit 1 on a miss
  published LABEL BANDS MACHINE FILE [--published-tries PUB] [--arm-o ARM_O]
            [--qps-windows LOG [--new-arm NAME]] [--figures KINDS]
                               judge FILE against BANDS' [published.MACHINE]
                               rows (ClickBench's published figures), print
                               the table, exit 1 on a miss; FILE is a kit
                               results.json whose band_sections selected
                               published, or one in ClickBench's results
                               format (`result` rows of three tries,
                               `concurrent_qps`) whose machine is MACHINE;
                               refuses a kit results.json that did not
                               select published, and a FILE with no
                               readable QPS when MACHINE's row has one.
                               --published-tries PUB: the published results
                               file itself (ClickBench format, MACHINE, its
                               hot and cold equal to BANDS' rows); a
                               statement whose published tries 2 and 3
                               differ by more than statement_max_ratio has
                               no published hot reference, and its hot is
                               judged against ARM_O's instead
                               (ADR-0954 M8). --arm-o ARM_O: published
                               v0.28.0 through the same 43 x 3 cycle on the
                               same box and data, in either format FILE
                               takes; needs --published-tries. Without
                               ARM_O such a statement is UNJUDGED and the
                               run incomplete: exit 4 when nothing missed.
                               --qps-windows LOG: same-box concurrent
                               windows, one line each, `window=N arm=A
                               qps=Q err_ratio=R [key=value...]`; arm old
                               is v0.28.0 and --new-arm (default new) names
                               the other. Replaces the published QPS gate:
                               mean new QPS over mean old QPS must be at
                               least 0.95 and mean new error ratio no
                               higher than mean old; mean new QPS against
                               concurrent_qps_min_ratio x published is
                               printed, REPORTED and not gated. Refuses a
                               LOG with fewer than two windows of either
                               arm, a missing or non-numeric figure, or an
                               arm name it never holds
                               --figures KINDS: hot, cold or hot,cold
                               (default): which per-statement figures are
                               judged, for a pre-registration that names
                               only some. A kind left out is not judged and
                               one SKIPPED line per kind says how many
                               statement rows it covers; the concurrent
                               QPS row is unaffected
  comparable RUN_TOML A.json B.json
                               refuse two arms that are not comparable, with
                               RUN_TOML's server_flags_a and server_flags_b
                               as the only extra flags allowed to differ and
                               a resolved server setting allowed to differ
                               only when one of those flags sets it; print
                               each accepted difference
  relative BANDS A.json B.json [RUN_TOML]
                               refuse two arms that are not comparable, else
                               evaluate the lane's relative bands, print the
                               table, exit 1 on a miss and 5 when the box
                               speed band calls the run INVALID (every row is
                               still printed); without RUN_TOML the arms'
                               extra server flags must be equal and the box
                               speed band is enforced
  box-speed LOAD_TIME          print the box_speed provenance stamp read from
                               the load step's /usr/bin/time output; a
                               missing or unparsable figure is null with a
                               reason
  fetch-policy LOG...          print the logs fetch policy every server log
                               resolved, refusing a log without exactly one
                               such line or two logs that disagree
  gcp-names RUN_ID STORE_BUCKET_PREFIX
                               print a GCP run's instance name, store bucket
                               and label value, one per line
  gcs-next-page PAGE           print a GCS objects.list page's
                               nextPageToken, URL-encoded; empty on the last
  footprint PAGE...            print the store bucket's objects and bytes from
                               its objects.list pages as JSON, by tenant
                               prefix and key class
  load-window PROGRESS_LOG     print the load window's step seconds (load,
                               compact, fold, declare) and their sum
  upstream-result RESULTS PROGRESS_LOG
                               print a GCP run as one ClickBench results file
  check-prereg PREREG          refuse a pre-registration that does not name
                               every figure prereg judges exactly once
  prereg PREREG RESULTS PROGRESS_LOG UPSTREAM STORE_BUCKET
                               judge a GCP run against its pre-registration;
                               exit 1 on a miss, 2 when a figure is missing
                               or emitted twice
  memory-gap BANDS A.json A.tgz B.json B.tgz RUN_TOML
                               judge ADR-1170's memory gap (allocated minus
                               the unique tracked bytes) on arm B's gated ok
                               concurrent /metrics scrapes per round and
                               pooled, print
                               arm A's figures as context, exit 1 on a miss;
                               skipped when BANDS leaves either adr1170_* key
                               out or a placeholder

results.json carries SqlLatencyReport's statement shape
(crates/ravel-bench/src/sql_latency.rs), so the runbook's analyse.py
(docs/internal/clickbench-aws-runbook.md section 9) reads it unchanged:
`entries` holds one EntryReport per answered statement (id, min_ms, median_ms,
max_ms, cold_ms, rows_returned, per_run_accounting) and `failed` one
FailedEntry per statement that did not answer (id, run, error). Both carry the
kit's `answer` check and `tries_ms`, each try's milliseconds in try order
(null for a try that did not answer, in failed), and entries carry
`"error": null`. per_run_accounting is
null: an HTTP answer carries no executor accounting, as on the Flight lane.
min_ms (hot) is the best of tries 2 and 3 and cold_ms is try 1; median_ms and
max_ms run over all three tries. The report says so in its own
timing_definition string, because SqlLatencyReport's min_ms runs over all
three. The kit's own blocks sit beside them: provenance, totals, error_pooled
(the benchmark phase's calls only), concurrent and q29_probe
(null when the phase did not run), healthz (each sampled phase's /healthz
latency per port and heartbeat age; optional), phases and bands.
"""

import calendar
import decimal
import hashlib
import json
import math
import os
import random
import re
import statistics
import sys
import tarfile
import time
import traceback

PLACEHOLDER_PREFIX = "FILL-IN"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NAME_RE = re.compile(r"^[a-z][a-z0-9_-]*$")

# Phases box/run.sh implements, each a phase_NAME function there.
KNOWN_PHASES = ("benchmark", "concurrent", "q29_probe", "perf")
# Phases whose calls error_pooled counts. Only the benchmark phase's, so the
# figure and its bands do not change with which other phases a run names.
POOLED_PHASES = ("benchmark",)

# Band file sections a run.toml can select. `absolute` covers the [absolute]
# table and every [absolute.*] table under it, `published` the [published]
# table and its [published.<machine>] tables. A run.toml without
# band_sections selects DEFAULT_BAND_SECTIONS, as box/run.sh does.
BAND_SECTIONS = ("relative", "absolute", "published")
DEFAULT_BAND_SECTIONS = ("relative", "absolute")

STATEMENT_KEYS = tuple("q%02d" % n for n in range(1, 44))

# ClickBench's published figures as a band (ADR-4034 D6), in the band files of
# PUBLISHED_LANES. Optional: a band file may hold no published table. When it
# holds any, [published] carries PUBLISHED_KEYS and each [published.<machine>]
# every PUBLISHED_MACHINE_KEYS key: source as <file>@<commit>, concurrent_qps,
# and per statement [hot, cold] seconds, PUBLISHED_NULL where the published
# run did not answer.
PUBLISHED_LANES = ("rlog",)
PUBLISHED_KEYS = ("statement_max_ratio", "concurrent_qps_min_ratio")
PUBLISHED_MACHINE_KEYS = ("source", "concurrent_qps") + STATEMENT_KEYS
FIGURE_KINDS = ("hot", "cold")
PUBLISHED_NULL = "null"
PUBLISHED_SOURCE_RE = re.compile(r"^\S+\.json@[0-9a-f]{40}$")
# Same-box concurrent QPS (ADR-0954 M8): the v0.28.0 arm's name in a windows
# log, the default name of the arm judged against it, the bar on mean new
# QPS over mean old QPS, and the fewest windows each arm needs.
QPS_OLD_ARM = "old"
QPS_NEW_ARM = "new"
SAME_BOX_QPS_MIN_RATIO = "0.95"
QPS_MIN_WINDOWS = 2
# published's exit when nothing missed and a statement could not be judged.
EXIT_INCOMPLETE = 4
MACHINE_RE = re.compile(r"^[a-z0-9][a-z0-9.-]*$")
# The kit's own directory: on the box the staged kit, whose bands/ holds the
# band files launch.sh copied in.
KIT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Each lane the kit runs, by table: the keys its band file must hold and the
# only ones it may hold. The parquet lane's bands/parquet.toml belongs to
# clickbench_parquet_bench, which runs no kit phase yet.
LANE_BANDS = {
    "rlog": {
        "relative": (
            "error_pooled_max_delta", "qps_min_ratio", "hot_statement_max_ratio",
            "hot_statement_floor_s", "hot_sum_max_ratio", "cold_sum_max_ratio",
            "min_both_answered", "concurrent_error_pooled_max_delta", "q29_probe_max_ratio",
        ),
        "absolute": ("hot_sum_s_max", "cold_sum_s_max", "error_pooled_max",
                     "concurrent_qps_min", "concurrent_error_pooled_max", "q29_probe_s_max"),
        "absolute.hot_s_max": STATEMENT_KEYS,
    },
}
# Keys a lane's band file may leave out, or leave a placeholder in a selected
# section: either way that band is skipped and adds no row.
OPTIONAL_LANE_BANDS = {
    "rlog": {
        "relative": ("healthz_p99_max_ratio_4318", "healthz_p99_max_ratio_4319",
                     "adr1170_gap_max_bytes", "adr1170_gap_share_above_max",
                     "box_speed_min_ratio", "box_speed_max_ratio"),
        "absolute": ("healthz_p99_ms_max_4318", "healthz_p99_ms_max_4319"),
    },
}
HEALTHZ_PORTS = ("4318", "4319")
# The relative box speed band's keys, lower and upper bound on arm B's load
# user CPU over arm A's; both or neither.
BOX_SPEED_KEYS = ("box_speed_min_ratio", "box_speed_max_ratio")
# kit.py relative's exit, and fetch.sh's, when the box speed band calls the
# run invalid: distinct from 1 (a band missed) and 2 (a refusal).
EXIT_INVALID = 5
# The phases run.toml's healthz_phases may name for the /healthz sampler; the
# default samples both.
HEALTHZ_PHASES = ("benchmark", "concurrent")
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
# The phase keys box/run.sh reads, with the defaults launch.sh writes into the
# run.toml it stages when the file leaves them out.
RUN_DEFAULTS = (
    ("concurrent_rounds", 3), ("concurrent_connections", 10), ("concurrent_duration_s", 600),
    ("concurrent_seed", "clickbench"), ("q29_probe_warmup", 1), ("q29_probe_runs", 3),
    ("perf_record", False),
)
# Read by box/run.sh (band_sections through query_timeout_s, server_flags_a,
# server_flags_b, healthz_phases and the RUN_DEFAULTS keys), launch.sh
# (band_sections, ami through key_name) or fetch.sh (box_speed_band) when
# present.
RUN_OPTIONAL = (
    "band_sections", "window_hours", "audit_text", "expected_statements",
    "fold_min_entry_ratio", "query_timeout_s", "ami", "subnet",
    "security_group", "key_name", "server_flags_a", "server_flags_b",
    "healthz_phases", "box_speed_band", "cloud", "store",
) + tuple(k for k, _ in RUN_DEFAULTS)
# Where a run's boxes run (`cloud`) and the store Ravel writes to there
# (`store`). The AWS kit (launch.sh, two arms built from source, loopback
# RustFS) is the default; the GCP path (launch-gcp.sh) runs one arm of a
# published release against a GCS bucket.
CLOUD_STORES = {"aws": "rustfs", "gcp": "gcs"}
# A GCP run.toml's keys: GCP_REQUIRED must be present, GCP_OPTIONAL may be,
# and any other key, a table among them, is refused. It has no lanes, [bands]
# or band_sections: a single arm has no relative bands, and its
# pre-registered bands live in the file `prereg` names (kit.py prereg).
GCP_REQUIRED = (
    "cloud", "store", "release_tag", "sha_a", "ravel_server_sha256", "ravel_cli_sha256",
    "phases", "project", "region", "zone", "instance_type", "image_family", "image_project",
    "service_account", "bucket", "prefix", "store_bucket_prefix", "tenant", "shards", "corpus",
    "mapping", "dataset_url", "dataset_sha256", "rows_expected", "memory_budget_bytes",
    "audit_mode", "prereg",
)
GCP_OPTIONAL = (
    "boot_disk_gb", "release_url", "window_hours", "audit_text", "expected_statements",
    "fold_min_entry_ratio", "query_timeout_s", "server_flags_a", "healthz_phases",
) + tuple(k for k, _ in RUN_DEFAULTS)
GCP_DEFAULT_BOOT_DISK_GB = 100
GCP_MIN_BOOT_DISK_GB = 50
DEFAULT_RELEASE_URL = "https://github.com/NOFireAI/ravel/releases/download"
RELEASE_TAG_RE = re.compile(r"^v[0-9]+\.[0-9]+\.[0-9]+$")
GCP_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*[a-z0-9]$")
SERVICE_ACCOUNT_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]@([a-z][a-z0-9-]+)"
                                r"\.iam\.gserviceaccount\.com$")
# A store bucket is <store_bucket_prefix>-<run id, lowercased, without its
# "clickbench-" head>, at most 63 characters (gcp-names).
STORE_BUCKET_PREFIX_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,30}[a-z0-9]$")
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$")
RUN_ID_RE = re.compile(r"^clickbench-([0-9]{8}T[0-9]{6}Z-[0-9]+-[0-9]+)$")
# Extra ravel-server flags per arm, appended by box/run.sh after the flags
# KIT_SET_FLAGS names; an entry repeating one of those is refused.
ARM_FLAG_KEYS = {"A": "server_flags_a", "B": "server_flags_b"}
KIT_SET_FLAGS = (
    "--store", "--shards", "--listen-http", "--listen-health", "--memory-budget-bytes",
    "--audit-mode", "--audit-text", "--tenant-token-file", "--tenant-hash-unkeyed",
    "--gcs-bucket",
)
# For each STAMPED_SETTINGS value, the flag that sets it directly and, for the
# two SQL pools, the flag on the other pool that resolve_performance_defaults
# in services/ravel-server/src/config.rs reconciles it against. The server
# logs each value's source but not the flag's name, so the mapping lives here.
# It is not every flag that can move a value: --memory-budget-bytes (which the
# kit sets, so base_flags pins it), --cache-max-bytes,
# --catalog-cache-max-bytes, --disable-cache and --mode gateway also move the
# derived pools through the memory remainder. The check fails closed for
# those: a value they move differs with no entry here to explain it, or not as
# an entry here would move it, and is refused.
SETTING_FLAGS = {
    "cache_max_bytes": ("--cache-max-bytes",),
    "catalog_cache_max_bytes": ("--catalog-cache-max-bytes",),
    "fetch_concurrency": ("--fetch-concurrency",),
    "sql_max_query_bytes": ("--sql-max-query-bytes", "--sql-tenant-max-bytes"),
    "sql_tenant_max_bytes": ("--sql-tenant-max-bytes", "--sql-max-query-bytes"),
}
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
    "error_pooled", "concurrent", "q29_probe", "phases", "bands",
)
CONCURRENT_ROUND_FIELDS = (
    "round", "duration_s", "wall_s", "connections", "calls", "ok", "err", "wrong_answer",
    "qps", "error_ratio",
)
CONCURRENT_POOLED_FIELDS = (
    "rounds", "calls", "ok", "err", "wrong_answer", "error_ratio_weighted",
    "error_ratio_mean_of_rounds", "qps_pooled",
)
Q29_PROBE_FIELDS = ("statement", "warmup", "runs", "runs_s", "errors", "min_s", "median_s")
CONCURRENT_DEFINITION = (
    "upstream ClickBench bench_concurrent_qps: a call succeeds on a 2xx answer whose transfer "
    "completed (curl exit 0); a round's qps is its successful calls over the nominal "
    "duration_s (0 with none, null when it made no call) and its error_ratio its failed calls "
    "over all its calls; qps_pooled is the mean of the non-null round qps, error_ratio_weighted all rounds' failed calls over all their "
    "calls, error_ratio_mean_of_rounds the mean of the non-null round error_ratio")
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
# path and SHA live in provenance.binary, git_sha and server_flags.binary,
# which are not compared because they are what differs between the arms by
# design. check_comparable compares server_flags' base_flags and extra_flags
# itself. For arms whose declared extra flags differ, a server_settings key may
# differ only when SETTING_FLAGS names a flag for it that the two arms'
# declared lists set differently, and then only to the value that flag
# resolves it to; every other difference is refused.
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
    sections = data.get("band_sections", list(DEFAULT_BAND_SECTIONS))
    if not isinstance(sections, list) or not sections:
        raise Refusal("band_sections must be a non-empty array")
    for s in sections:
        if s not in BAND_SECTIONS:
            raise Refusal("band_sections holds %r; known sections: %s"
                          % (s, ", ".join(BAND_SECTIONS)))
    return sections


def run_cloud(data, path):
    """The run's cloud, after refusing a cloud or store the kit does not run
    or a store that cloud does not run."""
    cloud = data.get("cloud", "aws")
    if cloud not in CLOUD_STORES:
        raise Refusal("run.toml %s: cloud must be one of %s, got %r"
                      % (path, ", ".join(sorted(CLOUD_STORES)), cloud))
    store = data.get("store", CLOUD_STORES[cloud])
    if store != CLOUD_STORES[cloud]:
        raise Refusal("run.toml %s: cloud %s runs store %s, got store %r"
                      % (path, cloud, CLOUD_STORES[cloud], store))
    return cloud


def check_run(path):
    data = parse_toml(path)
    if run_cloud(data, path) == "gcp":
        return check_run_gcp(path, data)
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
    for key in ("dataset_sha256", "rustfs_sha256"):
        if not SHA256_RE.match(str(data[key])):
            raise Refusal("run.toml %s: %s must be 64 lowercase hex digits, got %r"
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
    if "benchmark" not in data["phases"]:
        raise Refusal("run.toml %s: phases must include benchmark: results.json's statements "
                      "come from it" % path)
    _check_run_phase_keys(path, data)
    for lane in data["lanes"]:
        if lane not in LANE_BANDS:
            raise Refusal("run.toml %s: lane %r is not one the kit runs (%s)"
                          % (path, lane, ", ".join(sorted(LANE_BANDS))))
    bands = data.get("bands")
    for lane in data["lanes"]:
        if not isinstance(bands, dict) or not isinstance(bands.get(lane), str):
            raise Refusal("run.toml %s: lane %r has no band file under [bands]" % (path, lane))
    try:
        sections = band_sections(data)
        if "published" in sections:
            if "concurrent" not in data["phases"]:
                raise Refusal("band_sections selects published, which judges concurrent QPS, "
                              "and phases does not include concurrent")
            for lane in data["lanes"]:
                published_table(lane, run_band_file(bands[lane], lane),
                                str(data["instance_type"]))
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
    for key in sorted(ARM_FLAG_KEYS.values()):
        flags = data.get(key, [])
        if not isinstance(flags, list) or not all(isinstance(f, str) and f for f in flags):
            raise Refusal("run.toml %s: %s must be an array of non-empty strings, got %r"
                          % (path, key, flags))
        for f in flags:
            for kit_flag in KIT_SET_FLAGS:
                if f == kit_flag or f.startswith(kit_flag + "="):
                    raise Refusal("run.toml %s: %s repeats %s, which the kit sets itself"
                                  % (path, key, kit_flag))
    if not isinstance(data.get("box_speed_band", True), bool):
        raise Refusal("run.toml %s: box_speed_band must be true or false, got %r"
                      % (path, data["box_speed_band"]))
    sampled = healthz_phases(data)
    if (not isinstance(sampled, list) or len(set(map(str, sampled))) != len(sampled)
            or any(p not in HEALTHZ_PHASES for p in sampled)):
        raise Refusal("run.toml %s: healthz_phases must be an array of distinct phases from "
                      "%s, got %r" % (path, ", ".join(HEALTHZ_PHASES), sampled))
    return data


def _check_run_phase_keys(path, data):
    """Refuse a RUN_DEFAULTS key of the wrong type, and phase perf without
    perf_record = true."""
    wrong = []
    for key, default in RUN_DEFAULTS:
        v = data.get(key, default)
        if isinstance(default, bool):
            if not isinstance(v, bool):
                wrong.append("%s must be true or false, got %r" % (key, v))
        elif isinstance(default, str):
            if not isinstance(v, str) or not v:
                wrong.append("%s must be a non-empty string, got %r" % (key, v))
        else:
            least = 0 if key == "q29_probe_warmup" else 1
            if not isinstance(v, int) or isinstance(v, bool) or v < least:
                wrong.append("%s must be a whole number of at least %d, got %r" % (key, least, v))
    if wrong:
        raise Refusal("run.toml %s: %s" % (path, "; ".join(wrong)))
    if "perf" in data["phases"] and data.get("perf_record") is not True:
        raise Refusal("run.toml %s: phase perf needs perf_record = true" % path)


def _positive_int(v):
    return isinstance(v, int) and not isinstance(v, bool) and v > 0


def check_run_gcp(path, data):
    """A GCP run.toml (cloud = "gcp"): one arm, the release_tag's published
    binaries, store gcs. Refuses a key or table outside GCP_REQUIRED and
    GCP_OPTIONAL by name, so an AWS-only key (sha_b, lanes, [bands],
    instance_profile, rustfs_url) is never silently ignored."""
    unknown = sorted(k for k in data if k not in GCP_REQUIRED and k not in GCP_OPTIONAL)
    if unknown:
        raise Refusal("run.toml %s: unknown key(s) or table(s) for cloud gcp: %s"
                      % (path, ", ".join(unknown)))
    missing = [k for k in GCP_REQUIRED if data.get(k) in (None, "")]
    if missing:
        raise Refusal("run.toml %s: missing required key(s): %s" % (path, ", ".join(missing)))
    if not SHA_RE.match(str(data["sha_a"])):
        raise Refusal("run.toml %s: sha_a must be a full 40-character commit, got %r"
                      % (path, data["sha_a"]))
    for key in ("dataset_sha256", "ravel_server_sha256", "ravel_cli_sha256"):
        if not SHA256_RE.match(str(data[key])):
            raise Refusal("run.toml %s: %s must be 64 lowercase hex digits, got %r"
                          % (path, key, data[key]))
    if not RELEASE_TAG_RE.match(str(data["release_tag"])):
        raise Refusal("run.toml %s: release_tag must look like v1.2.3, got %r"
                      % (path, data["release_tag"]))
    phases = data["phases"]
    if not isinstance(phases, list) or not phases:
        raise Refusal("run.toml %s: phases must be a non-empty array" % path)
    for phase in phases:
        if phase not in KNOWN_PHASES:
            raise Refusal("run.toml %s: phase %r is not one box/run.sh implements (%s)"
                          % (path, phase, ", ".join(KNOWN_PHASES)))
    if "benchmark" not in phases:
        raise Refusal("run.toml %s: phases must include benchmark: results.json's statements "
                      "come from it" % path)
    _check_run_phase_keys(path, data)
    for key in ("project", "region", "zone", "image_family", "image_project", "instance_type"):
        if not isinstance(data[key], str) or not GCP_NAME_RE.match(data[key]):
            raise Refusal("run.toml %s: %s %r is not a GCP name" % (path, key, data[key]))
    if not data["zone"].startswith(data["region"] + "-"):
        raise Refusal("run.toml %s: zone %s is not in region %s"
                      % (path, data["zone"], data["region"]))
    m = SERVICE_ACCOUNT_RE.match(str(data["service_account"]))
    if not m or m.group(1) != data["project"]:
        raise Refusal("run.toml %s: service_account %r is not a service account email in "
                      "project %s" % (path, data["service_account"], data["project"]))
    if not BUCKET_RE.match(str(data["bucket"])):
        raise Refusal("run.toml %s: bucket %r is not a bucket name" % (path, data["bucket"]))
    if not STORE_BUCKET_PREFIX_RE.match(str(data["store_bucket_prefix"])):
        raise Refusal("run.toml %s: store_bucket_prefix %r must be 3 to 32 lowercase letters, "
                      "digits and hyphens" % (path, data["store_bucket_prefix"]))
    if not re.match(r"^[A-Za-z0-9._/-]+$", str(data["prefix"])):
        raise Refusal("run.toml %s: prefix %r may hold only letters, digits, . _ / -"
                      % (path, data["prefix"]))
    for key in ("shards", "memory_budget_bytes", "rows_expected"):
        if not _positive_int(data[key]):
            raise Refusal("run.toml %s: %s must be a whole number above 0" % (path, key))
    disk = data.get("boot_disk_gb", GCP_DEFAULT_BOOT_DISK_GB)
    if not _positive_int(disk) or disk < GCP_MIN_BOOT_DISK_GB:
        raise Refusal("run.toml %s: boot_disk_gb must be a whole number of at least %d "
                      "(the dataset alone is 14.8 GB)" % (path, GCP_MIN_BOOT_DISK_GB))
    url = data.get("release_url", DEFAULT_RELEASE_URL)
    if not isinstance(url, str) or not re.match(r"^https://[A-Za-z0-9./_-]+$", url):
        raise Refusal("run.toml %s: release_url must be an https URL, got %r" % (path, url))
    hours = data.get("window_hours", DEFAULT_WINDOW_HOURS)
    if not _positive_int(hours):
        raise Refusal("run.toml %s: window_hours must be a whole number above 0" % path)
    if data["audit_mode"] not in ("off", "required"):
        raise Refusal("run.toml %s: audit_mode must be off or required" % path)
    if data.get("audit_text", "redacted") not in ("redacted", "plaintext"):
        raise Refusal("run.toml %s: audit_text must be redacted or plaintext" % path)
    if data.get("audit_text") == "plaintext" and data["audit_mode"] != "required":
        raise Refusal("run.toml %s: audit_text plaintext needs audit_mode required "
                      "(the server refuses it otherwise)" % path)
    flags = data.get("server_flags_a", [])
    if not isinstance(flags, list) or not all(isinstance(f, str) and f for f in flags):
        raise Refusal("run.toml %s: server_flags_a must be an array of non-empty strings, got %r"
                      % (path, flags))
    for f in flags:
        for kit_flag in KIT_SET_FLAGS:
            if f == kit_flag or f.startswith(kit_flag + "="):
                raise Refusal("run.toml %s: server_flags_a repeats %s, which the kit sets itself"
                              % (path, kit_flag))
        if f.startswith("--s3-"):
            raise Refusal("run.toml %s: server_flags_a holds %s, which --store gcs refuses"
                          % (path, f))
    sampled = healthz_phases(data)
    if (not isinstance(sampled, list) or len(set(map(str, sampled))) != len(sampled)
            or any(p not in HEALTHZ_PHASES for p in sampled)):
        raise Refusal("run.toml %s: healthz_phases must be an array of distinct phases from "
                      "%s, got %r" % (path, ", ".join(HEALTHZ_PHASES), sampled))
    return data


def run_band_file(named, lane):
    """The band file a run.toml names for LANE: the copy launch.sh staged in
    the kit when this kit.py runs from one (on the box), else NAMED, a
    relative path read from the repository root as launch.sh reads it."""
    staged = os.path.join(KIT_DIR, "bands", lane + ".toml")
    if os.path.isfile(staged):
        return staged
    if os.path.isabs(named):
        return named
    return os.path.join(KIT_DIR, "..", "..", "..", named)


def arm_extra_flags(data, arm):
    return list(data.get(ARM_FLAG_KEYS[arm], []))


def healthz_phases(data):
    return data.get("healthz_phases", list(HEALTHZ_PHASES))


def _flag_values(flags):
    """Each --flag in FLAGS mapped to the values given to it, in order: the
    part after "=", or the entries up to the next --flag."""
    out, cur = {}, None
    for f in flags:
        if f.startswith("--"):
            name, eq, val = f.partition("=")
            cur = out.setdefault(name, [])
            cur.append([val] if eq else [])
        elif cur is not None:
            cur[-1].append(f)
        else:
            out.setdefault("", [[]])[0].append(f)
    return out


def _setting_flag_value(flags, flag, arm):
    """The whole number ARM's parsed extra FLAGS give FLAG, None when they do
    not give it."""
    given = flags.get(flag)
    if given is None:
        return None
    if len(given) != 1 or len(given[0]) != 1 or not re.match(r"^[0-9]+$", given[0][0]):
        raise Refusal("arms are not comparable: %s's declared extra flags give %s %r, not one "
                      "whole number, so the kit cannot tell what it sets"
                      % (arm, flag, [" ".join(v) for v in given]))
    return int(given[0][0])


def _setting_rule(k, flags, arm):
    """How ARM's parsed extra FLAGS resolve server setting K, as
    resolve_performance_defaults does, against U, the value K holds with
    neither of its SETTING_FLAGS set (the same on both arms): ("exact", v) is
    v whatever U is, ("unflagged", None) is U, ("raise", v) is max(U, v) and
    ("clamp", v) is min(U, v). The third element says why, for a refusal."""
    names = SETTING_FLAGS[k]
    d = _setting_flag_value(flags, names[0], arm)
    o = _setting_flag_value(flags, names[1], arm) if len(names) > 1 else None
    if d is not None:
        if k == "sql_max_query_bytes" and o is not None and d > o:
            return ("exact", o, "%s's %s %d is clamped to its %s %d"
                    % (arm, names[0], d, names[1], o))
        return ("exact", d, "%s's %s %d sets it to %d" % (arm, names[0], d, d))
    if o is None:
        return ("unflagged", None, "%s sets no %s, so it holds the unflagged value"
                % (arm, " or ".join(names)))
    if k == "sql_tenant_max_bytes":
        return ("raise", o, "%s's %s %d raises it to %d only from an unflagged value below %d"
                % (arm, names[1], o, o, o))
    return ("clamp", o, "%s's %s %d clamps it to %d only from an unflagged value above %d"
            % (arm, names[1], o, o, o))


def _check_setting(k, a_val, b_val, fa, fb):
    """Refuse server setting K unless some unflagged value, shared by both
    arms, resolves to A_VAL under arm A's parsed extra flags FA and to B_VAL
    under arm B's FB."""
    lo, hi, ok, why = 0, None, True, []
    for arm, v, flags in (("A", a_val, fa), ("B", b_val, fb)):
        if not isinstance(v, int) or isinstance(v, bool):
            raise Refusal("arms are not comparable: provenance.server_settings.%s is %r on %s, "
                          "not a whole number" % (k, v, arm))
        kind, f, reason = _setting_rule(k, flags, arm)
        why.append(reason)
        if kind == "exact":
            ok = ok and v == f
        elif kind != "unflagged" and v == f:
            # The unflagged value may sit anywhere on the far side of f.
            if kind == "raise":
                hi = f if hi is None else min(hi, f)
            else:
                lo = max(lo, f)
        elif (kind == "raise" and v < f) or (kind == "clamp" and v > f):
            ok = False
        else:
            lo, hi = max(lo, v), v if hi is None else min(hi, v)
    if not ok or (hi is not None and lo > hi):
        raise Refusal("arms are not comparable: provenance.server_settings.%s is A %r, B %r, "
                      "which the declared extra flags do not resolve it to: %s"
                      % (k, a_val, b_val, "; ".join(why)))


def _setting_differences(va, vb, extra_a, extra_b):
    """The accepted server_settings differences between two arms, one
    "KEY A x B y" string each. A key whose SETTING_FLAGS the arms' extra flags
    set differently must hold, on each arm, exactly the value those flags
    resolve it to; any other key may not differ."""
    fa, fb = _flag_values(extra_a), _flag_values(extra_b)
    moved = set(f for f in set(fa) | set(fb) if fa.get(f) != fb.get(f))
    out = []
    for k in sorted(set(va) | set(vb)):
        a_val, b_val = va.get(k), vb.get(k)
        touched = k in SETTING_FLAGS and moved.intersection(SETTING_FLAGS[k])
        if a_val == b_val and not touched:
            continue
        if k not in SETTING_FLAGS:
            raise Refusal("arms are not comparable: provenance.server_settings.%s differs "
                          "(A %r, B %r), and the kit knows no flag that sets it"
                          % (k, a_val, b_val))
        if not touched:
            raise Refusal("arms are not comparable: provenance.server_settings.%s differs "
                          "(A %r, B %r), and the declared extra flags do not set %s differently"
                          % (k, a_val, b_val, " or ".join(SETTING_FLAGS[k])))
        _check_setting(k, a_val, b_val, fa, fb)
        if a_val != b_val:
            out.append("%s A %r B %r" % (k, a_val, b_val))
    return out


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
    optional = OPTIONAL_LANE_BANDS.get(lane, {})
    sections = list(BAND_SECTIONS) if sections is None else sections
    data = parse_toml(path)
    published = sorted(t for t in data if lane in PUBLISHED_LANES
                       and isinstance(data[t], dict) and _section_of(t) == "published")
    removed, unknown = [], []
    for table, node in sorted(data.items()):
        if not isinstance(node, dict):
            (removed if table in REMOVED_BAND_KEYS else unknown).append(table)
            continue
        allowed = schema.get(table, ()) + optional.get(table, ())
        if table in published:
            allowed = PUBLISHED_KEYS if table == "published" else PUBLISHED_MACHINE_KEYS
        for key in sorted(node):
            name = "%s.%s" % (table, key)
            if key in REMOVED_BAND_KEYS:
                removed.append(name)
            elif key not in allowed:
                unknown.append(name)
    if removed:
        raise Refusal("band file %s: removed key(s) %s; delete them"
                      % (path, ", ".join(removed)))
    if unknown:
        raise Refusal("band file %s: unknown key(s) %s for lane %s"
                      % (path, ", ".join(unknown), lane))
    bad = [t for t in published if t != "published"
           and not MACHINE_RE.match(t.split(".", 1)[1])]
    if bad:
        raise Refusal("band file %s: table(s) %s do not name a machine"
                      % (path, ", ".join("[%s]" % t for t in bad)))
    required = dict(schema)
    if published:
        required["published"] = PUBLISHED_KEYS
        required.update((t, PUBLISHED_MACHINE_KEYS) for t in published if t != "published")
    missing = ["%s.%s" % (t, k) for t in sorted(required) for k in required[t]
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
    for table in sorted(optional):
        for key in optional[table]:
            v = data.get(table, {}).get(key)
            if v is not None and not _placeholder(v) and (not _num(v) or v < 0):
                malformed.append("%s.%s" % (table, key))
    if malformed:
        raise Refusal("band file %s: %s must be a number of at least 0"
                      % (path, ", ".join(malformed)))
    wrong = []
    for table in published:
        node = data[table]
        if table == "published":
            wrong += ["published.%s must be a number above 0, got %r" % (k, node[k])
                      for k in PUBLISHED_KEYS if not _num(node[k]) or node[k] <= 0]
            continue
        if not isinstance(node["source"], str) or not PUBLISHED_SOURCE_RE.match(node["source"]):
            wrong.append("%s.source must be <file>.json@<40-hex commit>, got %r"
                         % (table, node["source"]))
        if not _published_figure(node["concurrent_qps"]):
            wrong.append("%s.concurrent_qps must be a number of at least 0 or %r, got %r"
                         % (table, PUBLISHED_NULL, node["concurrent_qps"]))
        for key in STATEMENT_KEYS:
            v = node[key]
            if not isinstance(v, list) or len(v) != 2 or not all(map(_published_figure, v)):
                wrong.append("%s.%s must be [hot, cold], each a number of at least 0 or %r, "
                             "got %r" % (table, key, PUBLISHED_NULL, v))
    if wrong:
        raise Refusal("band file %s: %s" % (path, "; ".join(wrong)))
    rel = data.get("relative", {})
    speed = [rel.get(k) for k in BOX_SPEED_KEYS]
    if sum(1 for v in speed if _num(v)) == 1:
        raise Refusal("band file %s: set both of relative.%s or neither"
                      % (path, " and relative.".join(BOX_SPEED_KEYS)))
    if all(_num(v) for v in speed) and not 0 < speed[0] <= 1 <= speed[1]:
        raise Refusal("band file %s: relative.box_speed_min_ratio must be above 0 and at most "
                      "1, and relative.box_speed_max_ratio at least 1, got %r and %r"
                      % (path, speed[0], speed[1]))
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


def pooled_errors(phases):
    calls = errors = 0
    for name, phase in (phases or {}).items():
        if name not in POOLED_PHASES:
            continue
        calls += phase.get("calls", 0)
        errors += phase.get("errors", 0)
    return calls, errors


def run_metrics(report):
    calls, errors = pooled_errors(report.get("phases"))
    conc = report.get("concurrent")
    pooled = conc["pooled"] if isinstance(conc, dict) else None
    return {
        "hot_sum_s": report["totals"]["hot_sum_s"],
        "cold_sum_s": report["totals"]["cold_sum_s"],
        "error_pooled": (errors / calls) if calls else None,
        "concurrent": pooled,
        "q29_probe": report.get("q29_probe"),
    }


def _qps(pooled):
    """A concurrent phase's pooled QPS, 0.0 when no round made a call (each
    round's QPS is then null), with the note saying so."""
    if pooled["qps_pooled"] is None:
        return 0.0, "no round made a call"
    return pooled["qps_pooled"], ""


def _probe_failed(probe):
    if probe["errors"] or probe["min_s"] is None:
        return "%d of %d probe runs failed" % (probe["errors"], probe["runs"])
    return ""


def absolute_bands(lane, path, sections, report):
    """One arm judged against the lane's absolute section, when selected. A
    band whose phase did not run is inconclusive."""
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
    conc = m["concurrent"]
    label = "concurrent qps >= concurrent_qps_min"
    bound = ab["concurrent_qps_min"]
    if conc is None:
        out.append(_row("absolute", label, None, bound, "inconclusive", "no concurrent phase ran"))
    else:
        value, note = _qps(conc)
        out.append(_row("absolute", label, value, bound, "pass" if value >= bound else "miss", note))
    label = "concurrent error_ratio_weighted <= concurrent_error_pooled_max"
    bound = ab["concurrent_error_pooled_max"]
    if conc is None:
        out.append(_row("absolute", label, None, bound, "inconclusive", "no concurrent phase ran"))
    elif conc["error_ratio_weighted"] is None:
        out.append(_row("absolute", label, None, bound, "miss", "no concurrent call recorded"))
    else:
        value = conc["error_ratio_weighted"]
        out.append(_row("absolute", label, value, bound, "pass" if value <= bound else "miss"))
    probe = m["q29_probe"]
    label = "q29_probe min_s <= q29_probe_s_max"
    bound = ab["q29_probe_s_max"]
    if probe is None:
        out.append(_row("absolute", label, None, bound, "inconclusive", "no q29_probe phase ran"))
    elif _probe_failed(probe):
        out.append(_row("absolute", label, probe["min_s"], bound, "miss", _probe_failed(probe)))
    else:
        out.append(_row("absolute", label, probe["min_s"], bound,
                        "pass" if probe["min_s"] <= bound else "miss"))
    out += healthz_absolute(ab, report)
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


def _published_figure(v):
    return v == PUBLISHED_NULL or (_num(v) and v >= 0)


def published_table(lane, path, machine):
    """The lane band file's [published] table and its [published.MACHINE]
    table, refused when the file holds no published rows for MACHINE."""
    if lane not in PUBLISHED_LANES:
        raise Refusal("band file %s: lane %s has no published section (lanes with one: %s)"
                      % (path, lane, ", ".join(PUBLISHED_LANES)))
    data = check_bands(lane, path, ["published"])
    table = "published." + machine
    if table not in data:
        have = sorted(t.split(".", 1)[1] for t in data
                      if _section_of(t) == "published" and t != "published")
        raise Refusal("band file %s: band_sections selects published and there is no [%s] "
                      "table (machines with published rows: %s)"
                      % (path, table, ", ".join(have) or "none"))
    return data["published"], data[table]


def _hot_cold(tries):
    """Hot and cold seconds from one statement's three tries, in either
    format: hot the best of tries 2 and 3 that answered, None only when both
    failed; cold try 1, None when it failed."""
    hot = [t for t in tries[1:3] if _num(t)]
    return (min(hot) if hot else None, tries[0] if _num(tries[0]) else None)


def _kit_tries(path, row):
    """A kit results.json row's tries_ms as seconds, None for a try that did
    not answer."""
    tries = row.get("tries_ms")
    if not isinstance(tries, list) or len(tries) != 3 or not all(
            t is None or (_num(t) and t >= 0) for t in tries):
        raise Refusal("%s: statement %s has tries_ms %r, not three tries each a number of "
                      "at least 0 or null" % (path, row["id"], tries))
    return [None if t is None else t / 1000.0 for t in tries]


def _published_measured(path, report):
    """REPORT's figures as published_bands compares them: per statement
    number, (hot_s, cold_s) with None for a figure not answered, then the
    concurrent QPS, None when REPORT holds none, and a note saying why.
    REPORT is a kit results.json or one in ClickBench's results format
    (`result`, one row of three tries per statement, and `concurrent_qps`).
    Both formats take hot and cold from the three tries the same way
    (_hot_cold), so a statement with one failed try is judged on the tries
    that answered in both."""
    if "result" in report:
        rows = report["result"]
        if not isinstance(rows, list) or len(rows) != len(STATEMENT_KEYS) or not all(
                isinstance(r, list) and len(r) == 3
                and all(t is None or (_num(t) and t >= 0) for t in r) for r in rows):
            raise Refusal("%s: result must hold %d rows of three tries, each a number of "
                          "at least 0 or null" % (path, len(STATEMENT_KEYS)))
        figures = dict((n, _hot_cold(r)) for n, r in enumerate(rows, 1))
        if "concurrent_qps" not in report:
            return figures, None, "%s has no concurrent_qps field" % path
        qps = report["concurrent_qps"]
        if not _num(qps) or qps < 0:
            raise Refusal("%s: concurrent_qps must be a number of at least 0, got %r"
                          % (path, qps))
        return figures, qps, ""
    figures = {}
    for e in report["entries"] + report["failed"]:
        m = re.match(r"^q(\d\d)", e["id"])
        if not m:
            raise Refusal("%s: statement id %r has no qNN prefix" % (path, e["id"]))
        figures[int(m.group(1))] = _hot_cold(_kit_tries(path, e))
    conc = report["concurrent"]
    if conc is None:
        return figures, None, ("%s has no concurrent block: no concurrent phase ran, and "
                               "band_sections published needs one" % path)
    qps, note = _qps(conc["pooled"])
    return figures, qps, note


def _dec(v):
    return decimal.Decimal(repr(v))


def _ratio(m, p):
    return "%.4f" % (m / p) if p else "n/a"


def _check_machine(report, report_path, machine, published_selected=True):
    """Refuse REPORT when it names a machine other than MACHINE, and a kit
    results.json that did not select published when PUBLISHED_SELECTED."""
    if "result" in report:
        if report.get("machine") != machine:
            raise Refusal("%s: machine is %r, not %s; judge it against its own machine's "
                          "published rows" % (report_path, report.get("machine"), machine))
        return
    if published_selected and "published" not in report["provenance"]["band_sections"]:
        raise Refusal("%s: published not selected: its band_sections are %s"
                      % (report_path, ", ".join(report["provenance"]["band_sections"])))
    # box/run.sh records "unknown" when the instance metadata call fails,
    # which says nothing about the machine, so only a named one refuses.
    ran_on = report["provenance"].get("hardware", {}).get("instance_type", "unknown")
    if ran_on not in (machine, "unknown"):
        raise Refusal("%s: ran on %r, not %s; judge it against its own machine's "
                      "published rows" % (report_path, ran_on, machine))


def published_unstable(tries_path, tries_report, machine, pub, r):
    """The statements of the published results file TRIES_REPORT whose tries
    2 and 3 differ by more than R, as {number: (try 2, try 3)}. The file must
    be ClickBench's format for MACHINE and reproduce every [hot, cold] of the
    band table PUB, so the tries read are the ones the table was copied
    from."""
    if "result" not in tries_report:
        raise Refusal("%s: --published-tries takes a file in ClickBench's results format "
                      "(`result` rows of three tries)" % tries_path)
    _check_machine(tries_report, tries_path, machine)
    figures, _, _ = _published_measured(tries_path, tries_report)
    unstable = {}
    for n, key in enumerate(STATEMENT_KEYS, 1):
        want = tuple(None if v == PUBLISHED_NULL else v for v in pub[key])
        if figures[n] != want:
            raise Refusal("%s: statement %d is [hot, cold] %r, and the band table holds %r; "
                          "it is not the file the table was copied from"
                          % (tries_path, n, list(figures[n]), list(pub[key])))
        t2, t3 = tries_report["result"][n - 1][1:3]
        if _num(t2) and _num(t3):
            lo, hi = sorted((t2, t3))
            if _dec(hi) > _dec(r) * _dec(lo):
                unstable[n] = (t2, t3)
    return unstable


def _window_figure(path, lineno, fields, key, convert):
    if key not in fields:
        raise Refusal("%s:%d: no %s figure" % (path, lineno, key))
    try:
        v = convert(fields[key])
    except ValueError:
        raise Refusal("%s:%d: %s=%s is not a number" % (path, lineno, key, fields[key]))
    if not math.isfinite(v) or v < 0:
        raise Refusal("%s:%d: %s=%s is not a finite number of at least 0"
                      % (path, lineno, key, fields[key]))
    return v


def qps_windows(path):
    """A same-box windows log as {arm: [(qps, err_ratio), ...]}: one line per
    window, `window=N arm=A qps=Q err_ratio=R` and any further key=value
    fields; blank lines and # comments are skipped. Refuses a line that is
    not key=value fields, a missing or non-numeric figure, and a window
    number seen twice."""
    try:
        lines = open(path).read().splitlines()
    except OSError as e:
        raise Refusal("%s: not readable: %s" % (path, e))
    arms, seen = {}, set()
    for lineno, line in enumerate(lines, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = {}
        for token in line.split():
            k, eq, v = token.partition("=")
            if not eq or not k or k in fields:
                raise Refusal("%s:%d: %r is not one key=value field per key" % (path, lineno, token))
            fields[k] = v
        window = _window_figure(path, lineno, fields, "window", int)
        if window in seen:
            raise Refusal("%s:%d: window %d appears twice" % (path, lineno, window))
        seen.add(window)
        if not fields.get("arm"):
            raise Refusal("%s:%d: no arm" % (path, lineno))
        qps = _window_figure(path, lineno, fields, "qps", float)
        err = _window_figure(path, lineno, fields, "err_ratio", float)
        arms.setdefault(fields["arm"], []).append((fields["qps"], fields["err_ratio"], qps, err))
    return arms


def same_box_qps_bands(path, new_arm, published_qps, q):
    """The windows log PATH judged as ADR-0954 M8's concurrent QPS rows: mean
    NEW_ARM QPS over mean old QPS at least SAME_BOX_QPS_MIN_RATIO, and mean
    NEW_ARM error ratio no higher than mean old, both compared exactly in
    decimal. Mean new QPS against Q x PUBLISHED_QPS is a REPORTED row, not a
    gate."""
    arms = qps_windows(path)
    if new_arm == QPS_OLD_ARM:
        raise Refusal("--new-arm %s names the v0.28.0 arm itself; the gate would compare "
                      "it with itself" % new_arm)
    for arm in (QPS_OLD_ARM, new_arm):
        if arm not in arms:
            raise Refusal("%s: no window of arm %s (arms: %s)"
                          % (path, arm, ", ".join(sorted(arms)) or "none"))
        if len(arms[arm]) < QPS_MIN_WINDOWS:
            raise Refusal("%s: arm %s has %d window(s), and the gate needs at least %d"
                          % (path, arm, len(arms[arm]), QPS_MIN_WINDOWS))
    new, old = arms[new_arm], arms[QPS_OLD_ARM]
    n_new, n_old = decimal.Decimal(len(new)), decimal.Decimal(len(old))
    q_new = sum(decimal.Decimal(w[0]) for w in new)
    q_old = sum(decimal.Decimal(w[0]) for w in old)
    if q_old == 0:
        raise Refusal("%s: the old arm measured zero QPS in every window, so there is no "
                      "baseline to judge against" % path)
    e_new = sum(decimal.Decimal(w[1]) for w in new)
    e_old = sum(decimal.Decimal(w[1]) for w in old)
    mq_new, mq_old = float(q_new / n_new), float(q_old / n_old)
    me_new, me_old = float(e_new / n_new), float(e_old / n_old)
    bar = decimal.Decimal(SAME_BOX_QPS_MIN_RATIO)
    names = "%s (%d windows) over old v0.28.0 (%d windows)" % (new_arm, len(new), len(old))
    out = [
        _row("same-box", "concurrent qps mean %s / mean old >= %s" % (new_arm, bar),
             mq_new / mq_old if mq_old else None, float(bar),
             "pass" if q_new * n_old >= bar * q_old * n_new else "miss",
             "same-box qps: %s, mean %.4f over %.4f, ratio %s"
             % (names, mq_new, mq_old, _ratio(mq_new, mq_old))),
        _row("same-box", "concurrent err_ratio mean %s <= mean old" % new_arm,
             me_new, me_old, "pass" if e_new * n_old <= e_old * n_new else "miss",
             "same-box error ratio: %s, mean %.4f over %.4f, ratio %s"
             % (names, me_new, me_old, _ratio(me_new, me_old))),
    ]
    label = "concurrent qps >= %g x published (reported)" % q
    if published_qps == PUBLISHED_NULL:
        out.append(_row("published", label, mq_new, None, "reported",
                        "concurrent qps: mean %s %.3f, no published figure; not gated"
                        % (new_arm, mq_new)))
    else:
        out.append(_row("published", label, mq_new, float(_dec(q) * _dec(published_qps)),
                        "reported",
                        "concurrent qps: mean %s %.3f, published %.3f, ratio %s; not gated"
                        % (new_arm, mq_new, published_qps, _ratio(mq_new, published_qps))))
    return out


def published_bands(path, machine, report, report_path, tries=None, arm_o=None,
                    windows=None, figures_judged=FIGURE_KINDS, skipped=None):
    """One run judged against ClickBench's published figures for MACHINE in
    the band file PATH's [published] section (ADR-4034 D6). REPORT, read from
    REPORT_PATH, is a kit results.json whose band_sections selected
    published, or one in ClickBench's results format for MACHINE. Each
    statement figure the published row answered misses above
    statement_max_ratio times the
    published seconds, or when the run did not answer it; concurrent QPS
    misses below concurrent_qps_min_ratio times the published QPS. Both
    bounds are inclusive and compared in decimal, so a figure at exactly the
    factor passes. A run with no QPS to judge against a published one is
    refused, not judged.

    TRIES, a (path, report) pair, is the published results file; a statement
    whose published tries 2 and 3 differ by more than statement_max_ratio
    has its hot judged against ARM_O's hot (a (path, report) pair, v0.28.0
    on the same box and data) by the same factor, and is UNJUDGED without
    ARM_O (ADR-0954 M8). WINDOWS, a (path, new arm) pair, replaces the
    published QPS gate with same_box_qps_bands.

    Only the kinds in FIGURES_JUDGED are judged; a kind left out adds the
    count of its statement rows to SKIPPED (a dict) instead of a row."""
    _check_machine(report, report_path, machine)
    top, pub = published_table(lane_of(path), path, machine)
    figures, qps, qps_note = _published_measured(report_path, report)
    r, q = top["statement_max_ratio"], top["concurrent_qps_min_ratio"]
    unstable = published_unstable(tries[0], tries[1], machine, pub, r) if tries else {}
    arm_o_figures = None
    if arm_o:
        _check_machine(arm_o[1], arm_o[0], machine, published_selected=False)
        arm_o_figures, _, _ = _published_measured(arm_o[0], arm_o[1])
    out = []
    for n, key in enumerate(STATEMENT_KEYS, 1):
        for i, figure in enumerate(("hot", "cold")):
            p = pub[key][i]
            if p == PUBLISHED_NULL:
                continue
            if figure not in figures_judged:
                if skipped is not None:
                    skipped[figure] = skipped.get(figure, 0) + 1
                continue
            m = figures.get(n, (None, None))[i]
            if figure == "hot" and n in unstable:
                out.append(_arm_o_row(key, n, r, m, unstable[n], arm_o_figures))
                continue
            bound = _dec(r) * _dec(p)
            label = "%s %s_s <= %g x published" % (key, figure, r)
            if m is None:
                out.append(_row("published", label, None, float(bound), "miss",
                                "statement %d %s: not answered, published %.3f s"
                                % (n, figure, p), key))
                continue
            out.append(_row("published", label, m, float(bound),
                            "miss" if _dec(m) > bound else "pass",
                            "statement %d %s: measured %.3f s, published %.3f s, ratio %s"
                            % (n, figure, m, p, _ratio(m, p)), key))
    p = pub["concurrent_qps"]
    if windows:
        return out + same_box_qps_bands(windows[0], windows[1], p, q)
    label = "concurrent qps >= %g x published" % q
    if p == PUBLISHED_NULL:
        return out
    if qps is None:
        raise Refusal("%s; [published.%s] has concurrent_qps %s to judge it against"
                      % (qps_note, machine, p))
    bound = _dec(q) * _dec(p)
    note = "concurrent qps: measured %.3f, published %.3f, ratio %s" % (qps, p, _ratio(qps, p))
    out.append(_row("published", label, qps, float(bound), "miss" if _dec(qps) < bound else "pass",
                    "; ".join(s for s in (note, qps_note) if s)))
    return out


def _arm_o_row(key, n, r, m, tries, arm_o_figures):
    """Statement N's hot judged against arm O, its published tries 2 and 3
    (TRIES) differing by more than R; UNJUDGED when there is no arm O or arm
    O did not answer it."""
    why = ("published tries 2 and 3 %.3f s and %.3f s differ by more than %gx"
           % (tries[0], tries[1], r))
    label = "%s hot_s <= %g x arm O" % (key, r)
    if m is None:
        return _row("arm-o", label, None, None, "miss",
                    "statement %d hot: not answered; %s" % (n, why), key)
    o = arm_o_figures.get(n, (None, None))[0] if arm_o_figures is not None else None
    if o is None:
        reason = "no arm O" if arm_o_figures is None else "arm O did not answer it"
        return _row("arm-o", label, m, None, "unjudged",
                    "statement %d hot: unstable published reference, %s; %s"
                    % (n, why, reason), key)
    bound = _dec(r) * _dec(o)
    return _row("arm-o", label, m, float(bound), "miss" if _dec(m) > bound else "pass",
                "statement %d hot: measured %.3f s, arm O %.3f s, ratio %s; %s"
                % (n, m, o, _ratio(m, o), why), key)


def _differs(field, va, vb):
    return Refusal("arms are not comparable: provenance.%s differs (A %r, B %r)" % (field, va, vb))


def _base_flags_setting(flags):
    """Base flags as settings: the token file's path is where the box keeps
    the file under its root, not a setting, so it is not compared."""
    return [f if i == 0 or flags[i - 1] != "--tenant-token-file" else "<token file>"
            for i, f in enumerate(flags)]


def check_comparable(a, b, declared=None):
    """Refuse two arms whose provenance differs on a COMPARABLE field, their
    kit-set server flags (server_flags.base_flags, all but the token file's
    path) or their band sections.
    Their extra flags (server_flags.extra_flags) must be equal, or, with
    DECLARED (run.toml's server_flags_a and server_flags_b), must be exactly
    those lists. For arms whose extra flags differ that way, a server_settings
    key may differ only when one of its SETTING_FLAGS is set differently by
    those lists, and such a key must hold on each arm exactly the value that
    arm's list resolves it to. Returns one line per accepted difference, empty
    when there is none."""
    pa, pb = a["provenance"], b["provenance"]
    flags = {}
    for part in ("base_flags", "extra_flags"):
        field = "server_flags." + part
        va, vb = lookup(pa, field), lookup(pb, field)
        for arm, v in (("A", va), ("B", vb)):
            if not isinstance(v, list):
                raise Refusal("arms are not comparable: provenance.%s is missing on %s"
                              % (field, arm))
        flags[part] = (va, vb)
    base_a, base_b = (_base_flags_setting(f) for f in flags["base_flags"])
    if base_a != base_b:
        raise _differs("server_flags.base_flags", base_a, base_b)
    extra_a, extra_b = flags["extra_flags"]
    if declared is not None and [extra_a, extra_b] != list(declared):
        raise Refusal("arms are not comparable: provenance.server_flags.extra_flags is A %r, "
                      "B %r, and run.toml's server_flags_a and server_flags_b are A %r, B %r"
                      % (extra_a, extra_b, declared[0], declared[1]))
    if declared is None and extra_a != extra_b:
        raise _differs("server_flags.extra_flags", extra_a, extra_b)
    notes = []
    for field in COMPARABLE:
        va, vb = lookup(pa, field), lookup(pb, field)
        if va in (None, "") or vb in (None, ""):
            raise Refusal("arms are not comparable: provenance.%s is missing on %s"
                          % (field, "A" if va in (None, "") else "B"))
        if field == "server_settings" and extra_a != extra_b and isinstance(va, dict) \
                and isinstance(vb, dict):
            moved = _setting_differences(va, vb, extra_a, extra_b)
            if moved:
                notes.append("server_settings differs with them: %s" % ", ".join(moved))
            continue
        if va == vb:
            continue
        raise _differs(field, va, vb)
    if pa["band_sections"] != pb["band_sections"]:
        raise _differs("band_sections", pa["band_sections"], pb["band_sections"])
    if extra_a != extra_b:
        notes.insert(0, "arms differ by their declared extra server flags only: A %r, B %r"
                     % (extra_a, extra_b))
    return notes


def comparable(run_toml, a_json, b_json):
    """fetch.sh's comparability check, once per run: check_comparable against
    run.toml's declared extra flags, printing each accepted difference."""
    data = check_run(run_toml)
    declared = (arm_extra_flags(data, "A"), arm_extra_flags(data, "B"))
    for note in check_comparable(validate(a_json), validate(b_json), declared):
        print("comparable: %s" % note)


def declared_flags(run_toml):
    if run_toml is None:
        return None
    data = check_run(run_toml)
    return (arm_extra_flags(data, "A"), arm_extra_flags(data, "B"))


def box_speed_enforced(run_toml):
    return run_toml is None or check_run(run_toml).get("box_speed_band", True)


def _qs(ids):
    return " ".join(sid.split("_", 1)[0] for sid in sorted(ids))


def box_speed_stamp(path):
    """The box_speed provenance stamp from the load step's /usr/bin/time
    output (run.sh's format `wall=%e rss_kb=%M user=%U sys=%S`): each figure
    is null when the file is missing or does not hold it exactly once as a
    number, and reason then says why."""
    stamp = {"basis": "load_user_cpu_s", "load_user_cpu_s": None, "load_wall_s": None,
             "reason": None}
    try:
        text = open(path).read()
    except (OSError, IOError) as e:
        stamp["reason"] = "no load.time: %s" % e
        return stamp
    reasons = []
    for key, field in (("user", "load_user_cpu_s"), ("wall", "load_wall_s")):
        found = re.findall(r"(?:^|\s)%s=(\S*)" % key, text)
        if len(found) != 1:
            reasons.append("load.time carries %s= %d times, not once" % (key, len(found)))
            continue
        try:
            v = float(found[0])
        except ValueError:
            v = None
        if not _num(v) or v < 0:
            reasons.append("load.time %s=%r is not a number of seconds" % (key, found[0]))
            continue
        stamp[field] = v
    if reasons:
        stamp["reason"] = "; ".join(reasons)
    return stamp


def _box_cpu(report):
    """An arm's load user CPU seconds, else None and why."""
    stamp = report["provenance"].get("box_speed")
    if not isinstance(stamp, dict):
        return None, "no box_speed stamp"
    v = stamp.get("load_user_cpu_s")
    if not _num(v) or v <= 0:
        return None, "load_user_cpu_s is %r (%s)" % (v, stamp.get("reason") or "no reason given")
    return v, ""


def box_speed_relative(rel, a, b, enforced=True):
    """Arm B's load user CPU over arm A's, inside [box_speed_min_ratio,
    box_speed_max_ratio]. Both arms load the same rows, so outside it the
    boxes ran at different speeds and the verdict is invalid, not a miss;
    with ENFORCED false (run.toml box_speed_band = false) it is report. A
    missing stamp on either arm is inconclusive."""
    lo, hi = (rel.get(k) for k in BOX_SPEED_KEYS)
    if not (_num(lo) and _num(hi)):
        return []
    label = "box_speed load_user_cpu_s B / A"
    bound = "%g..%g" % (lo, hi)
    va, why_a = _box_cpu(a)
    vb, why_b = _box_cpu(b)
    if va is None or vb is None:
        why = "; ".join("arm %s: %s" % (arm, w) for arm, w in (("A", why_a), ("B", why_b)) if w)
        return [_row("relative", label, None, bound, "inconclusive", why)]
    ratio = vb / va
    notes = ["A %.1f s, B %.1f s" % (va, vb)]
    if a["provenance"]["git_sha"] != b["provenance"]["git_sha"]:
        notes.append("the loader differs between arms (built from each arm's sha)")
    if lo <= ratio <= hi:
        verdict = "pass"
    elif enforced:
        verdict = "invalid"
        notes.append("box speeds differ: re-run, do not read the bands")
    else:
        verdict = "report"
        notes.append("outside the band; run.toml box_speed_band = false makes it report-only")
    return [_row("relative", label, ratio, bound, verdict, "; ".join(notes))]


def relative_bands(path, a, b, declared=None, box_speed_band=True):
    """Arm B judged against arm A, over the statements both arms answered. A
    statement A answered and B did not is a miss of its own, and so is a
    both-answered set smaller than min_both_answered, an arm that answered
    nothing, or a statement whose row count differs between the arms. The box
    speed row comes first: an invalid one qualifies every row after it."""
    check_comparable(a, b, declared)
    sections = a["provenance"]["band_sections"]
    if "relative" not in sections:
        return []
    rel = check_bands(lane_of(path), path, sections)["relative"]
    out = box_speed_relative(rel, a, b, box_speed_band)

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
    absent = [arm for arm, m in (("A", ma), ("B", mb)) if m["concurrent"] is None]
    ca, cb = ma["concurrent"], mb["concurrent"]
    r = rel["qps_min_ratio"]
    label = "qps_pooled >= %g x A" % r
    if absent:
        add(label, None, None, "inconclusive", "no concurrent phase ran on %s" % " or ".join(absent))
    elif ca["qps_pooled"] is None or not ca["ok"]:
        add(label, _qps(cb)[0], None, "inconclusive", "arm A had no successful concurrent call")
    else:
        value, note = _qps(cb)
        bound = r * ca["qps_pooled"]
        add(label, value, bound, "pass" if value >= bound else "miss", note)
    d = rel["concurrent_error_pooled_max_delta"]
    label = "concurrent error_ratio_weighted <= A + %g" % d
    if absent:
        add(label, None, None, "inconclusive", "no concurrent phase ran on %s" % " or ".join(absent))
    elif ca["error_ratio_weighted"] is None:
        add(label, cb["error_ratio_weighted"], None, "inconclusive",
            "arm A recorded no concurrent call")
    elif cb["error_ratio_weighted"] is None:
        add(label, None, None, "miss", "arm B recorded no concurrent call")
    else:
        bound = ca["error_ratio_weighted"] + d
        add(label, cb["error_ratio_weighted"], bound,
            "pass" if cb["error_ratio_weighted"] <= bound else "miss")
    pa, pb = ma["q29_probe"], mb["q29_probe"]
    r = rel["q29_probe_max_ratio"]
    label = "q29_probe min_s <= %g x A" % r
    absent = [arm for arm, p in (("A", pa), ("B", pb)) if p is None]
    if absent:
        add(label, None, None, "inconclusive", "no q29_probe phase ran on %s" % " or ".join(absent))
    elif _probe_failed(pa):
        add(label, pb["min_s"], None, "inconclusive", "arm A: " + _probe_failed(pa))
    elif _probe_failed(pb):
        add(label, pb["min_s"], r * pa["min_s"], "miss", "arm B: " + _probe_failed(pb))
    else:
        bound = r * pa["min_s"]
        add(label, pb["min_s"], bound, "pass" if pb["min_s"] <= bound else "miss")

    out += healthz_relative(rel, a, b)
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


def resolved_run(path, out):
    """run.toml with every RUN_DEFAULTS key it leaves out written in at its
    default, before the first table, so the box runs what the laptop saw."""
    data = check_run(path)
    lines = open(path).read().splitlines()
    at = next((i for i, l in enumerate(lines) if _strip_comment(l).startswith("[")), len(lines))
    fill = []
    for key, default in RUN_DEFAULTS:
        if key not in data:
            if isinstance(default, bool):
                v = "true" if default else "false"
            elif isinstance(default, str):
                v = json.dumps(default)
            else:
                v = str(default)
            fill.append("%s = %s" % (key, v))
    if fill:
        fill = ["# Defaults launch.sh wrote for the phase keys this file left out."] + fill + [""]
    with open(out, "w") as f:
        f.write("\n".join(lines[:at] + fill + lines[at:]) + "\n")


# --- concurrent, q29_probe and perf phases ---------------------------------

def permutation(seed, connection, nq):
    """Connection CONNECTION's statement order, as upstream ClickBench's
    bench_concurrent_qps draws it: the first 8 bytes of sha256("<seed>-<c>"),
    big-endian, seed random.Random, which shuffles range(nq)."""
    digest = hashlib.sha256(("%s-%s" % (seed, connection)).encode("utf-8")).digest()
    order = list(range(int(nq)))
    random.Random(int.from_bytes(digest[:8], "big")).shuffle(order)
    return order


def _jsonl(path):
    out = []
    for n, line in enumerate(open(path), 1):
        try:
            out.append(json.loads(line))
        except ValueError as e:
            raise Refusal("%s:%d is not a JSON line: %s" % (path, n, e))
    return out


def _ok_2xx(call):
    """Upstream's success: curl --fail-with-body exiting 0, so a 2xx answer
    whose transfer also completed (curl_exit 0)."""
    status = call.get("http_status")
    return (isinstance(status, int) and 200 <= status <= 299
            and call.get("curl_exit") == 0)


def _settle_answer(call, resp, expect):
    """run_query's reading of an answer, for a call it recorded with RQ_DEFER:
    a 2xx answer's row count against EXPECT, else the error body's errorType
    and error."""
    if _ok_2xx(call):
        try:
            rows = json.load(open(resp))["data"]["rows"]
            n = len(rows) if isinstance(rows, list) else None
        except (OSError, ValueError, KeyError, TypeError):
            n = None
        call["rows"] = n
        if n is None:
            call.update(outcome="error", refusal_class="unreadable_answer",
                        error="HTTP %d without a readable data.rows array" % call["http_status"])
        elif expect is not None and n != expect:
            call.update(outcome="error", refusal_class="wrong_answer",
                        error="answer has %d rows, expected %d" % (n, expect))
        else:
            call.update(outcome="ok", refusal_class="", error="")
        return
    try:
        d = json.load(open(resp))
        cls, err = str(d.get("errorType", "")), str(d.get("error", ""))[:300]
    except (OSError, ValueError, AttributeError):
        cls, err = "", ""
    call.update(outcome="error", refusal_class=cls or "unclassified", error=err)


def concurrent_answers(d):
    """Rewrite each DIR/conn-N.jsonl with every call's "pending" member (the
    response file, relative to DIR, and the expected row count) replaced by
    its settled rows, outcome, refusal_class and error. A settled ok answer
    after try 1 is removed, as run_query removes it. A failed transfer's
    pending member names its curl stderr file instead: its first 300 bytes
    are appended to the call's error unless curl timed out (exit 28), as
    run_query quotes them outside a deferred call, and the file is removed. Prints the number of calls
    settled and of wrong answers among them."""
    settled = wrong = 0
    for name in sorted(os.listdir(d)):
        if not re.match(r"^conn-[0-9]+\.jsonl$", name):
            continue
        path = os.path.join(d, name)
        calls = _jsonl(path)
        for c in calls:
            p = c.pop("pending", None)
            if p is None:
                continue
            if "stderr" in p:
                stderr = os.path.join(d, p["stderr"])
                try:
                    with open(stderr, "rb") as f:
                        text = f.read(300).decode("utf-8", "replace").rstrip("\n")
                    os.remove(stderr)
                except OSError:
                    text = ""
                if c.get("curl_exit") != 28:
                    c["error"] = "%s: %s" % (c.get("error", ""), text)
                continue
            resp = os.path.join(d, p["response"])
            _settle_answer(c, resp, p.get("expect"))
            settled += 1
            wrong += c["refusal_class"] == "wrong_answer"
            if c["outcome"] == "ok" and c.get("try") != 1 and os.path.exists(resp):
                os.remove(resp)
        with open(path + ".tmp", "w") as f:
            for c in calls:
                f.write(json.dumps(c) + "\n")
        os.replace(path + ".tmp", path)
    print("settled %d calls, %d wrong answers" % (settled, wrong))


# A round whose wall time exceeds its nominal duration by more than this
# fraction is marked "overran".
OVERRUN_FRACTION = 0.10


def concurrent_round(meta, calls):
    """One round's figures. Its qps divides by the nominal duration while a
    call in flight at the deadline still counts, so a round that ran well past
    its deadline reads high; such a round carries "overran" with both
    figures, else null. The formula itself follows upstream."""
    duration = meta["duration_s"]
    ok = sum(1 for c in calls if _ok_2xx(c))
    err = len(calls) - ok
    overran = None
    if meta["wall_s"] > duration * (1 + OVERRUN_FRACTION):
        overran = {"wall_s": meta["wall_s"], "duration_s": duration}
    return {
        "round": meta["round"], "duration_s": duration, "wall_s": meta["wall_s"],
        "connections": meta["connections"], "calls": len(calls), "ok": ok, "err": err,
        "wrong_answer": sum(1 for c in calls if c.get("refusal_class") == "wrong_answer"),
        "qps": ok / float(duration) if calls else None,
        "error_ratio": err / float(len(calls)) if calls else None,
        "overran": overran,
    }


def pool_rounds(rounds):
    calls = sum(r["calls"] for r in rounds)
    err = sum(r["err"] for r in rounds)
    ratios = [r["error_ratio"] for r in rounds if r["error_ratio"] is not None]
    qps = [r["qps"] for r in rounds if r["qps"] is not None]
    return {
        "rounds": len(rounds), "calls": calls, "ok": sum(r["ok"] for r in rounds), "err": err,
        "wrong_answer": sum(r["wrong_answer"] for r in rounds),
        "error_ratio_weighted": err / float(calls) if calls else None,
        "error_ratio_mean_of_rounds": sum(ratios) / len(ratios) if ratios else None,
        "qps_pooled": sum(qps) / len(qps) if qps else None,
    }


def concurrent_summary(d):
    """The results.json concurrent block from DIR/rounds.jsonl (one line per
    round run.sh ran) and DIR/calls.jsonl."""
    metas = _jsonl(os.path.join(d, "rounds.jsonl"))
    if not metas:
        raise Refusal("concurrent-summary: %s/rounds.jsonl records no round" % d)
    by_round = {}
    for c in _jsonl(os.path.join(d, "calls.jsonl")):
        if not isinstance(c.get("round"), int) or not isinstance(c.get("connection"), int):
            raise Refusal("concurrent-summary: a call in %s/calls.jsonl has no round and "
                          "connection: %r" % (d, c))
        by_round.setdefault(c["round"], []).append(c)
    unknown = sorted(set(by_round) - set(m["round"] for m in metas))
    if unknown:
        raise Refusal("concurrent-summary: calls for round(s) %s, which rounds.jsonl does not "
                      "record" % ", ".join(str(u) for u in unknown))
    rounds = [concurrent_round(m, by_round.get(m["round"], [])) for m in metas]
    return {
        "definition": CONCURRENT_DEFINITION,
        "seed": metas[0]["seed"],
        "connections": metas[0]["connections"],
        "duration_s": metas[0]["duration_s"],
        "statements": metas[0]["statements"],
        "rounds": rounds,
        "pooled": pool_rounds(rounds),
    }


def q29_summary(d):
    """The results.json q29_probe block from DIR/calls.jsonl: min and median
    over the timed runs that answered, never the warmup."""
    calls = _jsonl(os.path.join(d, "calls.jsonl"))
    runs = [c for c in calls if c.get("probe") == "run"]
    if not runs:
        raise Refusal("q29-summary: %s/calls.jsonl records no timed run" % d)
    good = [float(c["duration_s"]) for c in runs if c.get("outcome") == "ok"]
    return {
        "statement": runs[0]["statement"],
        "warmup": sum(1 for c in calls if c.get("probe") == "warmup"),
        "runs": len(runs),
        "runs_s": [c.get("duration_s") for c in runs],
        "errors": len(runs) - len(good),
        "min_s": min(good) if good else None,
        "median_s": statistics.median(good) if good else None,
    }


# --- /healthz sampler --------------------------------------------------------

HEALTHZ_INTERVAL_S = 0.25
METRICS_INTERVAL_S = 1.0
HEALTHZ_TIMEOUT_S = 2.0
HEARTBEAT_METRIC = "ravel_health_heartbeat_age_seconds"
# The memory gauges each /metrics sample keeps, so an over-budget run can be
# checked pointwise against ADR-1170's resident <= unique tracked + reserve.
MEMORY_METRICS = (
    "ravel_process_allocator_bytes", "ravel_memory_budget_bytes",
    "ravel_memory_reserved_bytes", "ravel_memory_handoff_overlap_bytes",
    "ravel_memory_gate_resident_bytes", "ravel_memory_gate_available_bytes",
    "ravel_cache_resident_bytes")
HEALTHZ_LINE_RE = re.compile(
    r"^HEALTHZ phase=(\S+) port=(\S+) n=([0-9]+) p50_ms=([0-9.]+) p99_ms=([0-9.]+) "
    r"max_ms=([0-9.]+) failures=([0-9]+)$")
HEARTBEAT_LINE_RE = re.compile(
    r"^HEARTBEAT phase=(\S+) n=([0-9]+) failures=([0-9]+) bad=([0-9]+) max_s=([0-9.]+)$")


def _gate(path):
    """The generation an "up G" gate file names, else None: down, missing,
    or caught mid-write."""
    try:
        with open(path) as f:
            parts = f.read().split()
    except OSError:
        return None
    if len(parts) == 2 and parts[0] == "up" and parts[1].isdigit():
        return int(parts[1])
    return None


def _heartbeat(body):
    ages = []
    for line in body.splitlines():
        if line.startswith(HEARTBEAT_METRIC + "{") or line.startswith(HEARTBEAT_METRIC + " "):
            try:
                ages.append(float(line.rsplit(None, 1)[1]))
            except (IndexError, ValueError):
                pass
    return max(ages) if ages else None


def _memory_gauges(body):
    """Every MEMORY_METRICS sample in a /metrics body, keyed by its series
    (name and labels as exposed)."""
    out = {}
    for line in body.splitlines():
        if line.split("{", 1)[0].split(" ", 1)[0] not in MEMORY_METRICS:
            continue
        try:
            series, value = line.rsplit(None, 1)
            out[series] = float(value)
        except ValueError:
            pass
    return out


def _probe(opener, url, metrics):
    """One GET with HEALTHZ_TIMEOUT_S as the client timeout. A non-200, a
    transport error or an answer slower than the timeout is not ok; answered
    is whether any HTTP status came back within the timeout."""
    import urllib.error
    rec = {"status": 0, "ok": False, "error": ""}
    t0 = time.monotonic()
    body = b""
    try:
        with opener.open(url, timeout=HEALTHZ_TIMEOUT_S) as r:
            rec["status"] = r.status
            body = r.read()
    except urllib.error.HTTPError as e:
        rec["status"] = e.code
        rec["error"] = "HTTP %d" % e.code
    except Exception as e:  # every transport failure is a failed sample
        rec["error"] = "%s: %s" % (type(e).__name__, e)
    ms = (time.monotonic() - t0) * 1000.0
    rec["answered"] = rec["status"] != 0 and ms <= HEALTHZ_TIMEOUT_S * 1000.0
    if rec["status"] == 200 and not rec["error"]:
        if ms > HEALTHZ_TIMEOUT_S * 1000.0:
            rec["error"] = "answered after %.0f ms, past the %.0f ms timeout" % (
                ms, HEALTHZ_TIMEOUT_S * 1000.0)
        elif metrics:
            text = body.decode("utf-8", "replace")
            age = _heartbeat(text)
            if age is None:
                rec["error"] = "no %s in /metrics" % HEARTBEAT_METRIC
            rec["heartbeat_s"] = age
            rec["memory"] = _memory_gauges(text)
        rec["ok"] = not rec["error"]
    if not metrics:
        rec["ms"] = round(ms, 3)
    return rec


def healthz_sampler(d, gate, ack, base_4318, base_4319):
    """Sample GET /healthz on both listeners every HEALTHZ_INTERVAL_S and the
    heartbeat age from /metrics on 4318 every METRICS_INTERVAL_S, each on its
    own thread, into DIR/4318.jsonl, DIR/4319.jsonl and DIR/metrics.jsonl,
    until killed. Each record is one write to an O_APPEND file, so a kill
    never leaves half a line. A sample is gated (counted by healthz-summary) only when GATE read
    "up G" with the same G before and after it. A new generation is sampled
    at once; once every thread holds a gated sample of it, G is written to
    ACK, which run.sh waits for before its timed window opens."""
    import threading
    import urllib.request
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    targets = (("4318", base_4318.rstrip("/") + "/healthz", False, HEALTHZ_INTERVAL_S),
               ("4319", base_4319.rstrip("/") + "/healthz", False, HEALTHZ_INTERVAL_S),
               ("metrics", base_4318.rstrip("/") + "/metrics", True, METRICS_INTERVAL_S))
    os.makedirs(d, exist_ok=True)
    lock = threading.Lock()
    done = {}
    acked = [None]

    def note(name, gen):
        with lock:
            done[name] = gen
            if acked[0] != gen and all(done.get(t[0]) == gen for t in targets):
                with open(ack + ".tmp", "w") as f:
                    f.write("%d\n" % gen)
                os.replace(ack + ".tmp", ack)
                acked[0] = gen

    def loop(name, url, metrics, interval):
        fd = os.open(os.path.join(d, name + ".jsonl"), os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                     0o644)
        sampled = None
        while True:
            g0 = _gate(gate)
            t0 = time.monotonic()
            wall = time.time()
            rec = _probe(opener, url, metrics)
            g1 = _gate(gate)
            gated = g0 is not None and g0 == g1
            rec.update(t=round(wall, 3), mono=round(t0, 3), gen=g0 if gated else None,
                       gated=gated)
            os.write(fd, (json.dumps(rec, sort_keys=True) + "\n").encode("utf-8"))
            if gated:
                sampled = g0
                note(name, g0)
            while time.monotonic() - t0 < interval:
                g = _gate(gate)
                if g is not None and g != sampled:
                    break
                time.sleep(0.01)

    for t in targets:
        th = threading.Thread(target=loop, args=t)
        th.daemon = True
        th.start()
    while True:
        time.sleep(3600)


def _gated(path):
    if not os.path.exists(path):
        raise Refusal("healthz-summary: no %s" % path)
    return [r for r in _jsonl(path) if r.get("gated") is True]


def _nearest_rank(sorted_ms, q):
    return sorted_ms[max(0, int(math.ceil(q * len(sorted_ms))) - 1)]


def healthz_summary(phase, d):
    """The HEALTHZ line per port and the HEARTBEAT line for one phase, from
    the gated samples under DIR. A failed sample (a timeout, a non-200 or a
    transport error) counts as HEALTHZ_TIMEOUT_S and as a failure. A /metrics
    scrape that got no answer within the timeout counts as HEALTHZ_TIMEOUT_S
    of heartbeat age and as a failure: a stalled runtime is when the scrape
    fails, so it must not vanish from max_s. A scrape answered with a non-200
    or without a heartbeat age says nothing about a stall; it is counted as
    bad and kept out of max_s, and a phase in which every answered scrape was
    bad is refused, since its endpoint is misconfigured."""
    lines = []
    for port in HEALTHZ_PORTS:
        recs = _gated(os.path.join(d, port + ".jsonl"))
        if not recs:
            raise Refusal("healthz-summary: %s/%s.jsonl holds no gated sample for phase %s"
                          % (d, port, phase))
        ms = sorted(float(r["ms"]) if r.get("ok") else HEALTHZ_TIMEOUT_S * 1000.0 for r in recs)
        lines.append("HEALTHZ phase=%s port=%s n=%d p50_ms=%.3f p99_ms=%.3f max_ms=%.3f failures=%d"
                     % (phase, port, len(ms), _nearest_rank(ms, 0.50), _nearest_rank(ms, 0.99),
                        ms[-1], sum(1 for r in recs if not r.get("ok"))))
    scrapes = _gated(os.path.join(d, "metrics.jsonl"))
    if not scrapes:
        raise Refusal("healthz-summary: %s/metrics.jsonl holds no gated scrape for phase %s"
                      % (d, phase))
    ages, failures, bad = [], 0, []
    for r in scrapes:
        if r.get("ok") and _num(r.get("heartbeat_s")):
            ages.append(float(r["heartbeat_s"]))
        elif r.get("answered") is True:
            bad.append(r.get("error") or "HTTP %s" % r.get("status"))
        else:
            failures += 1
            ages.append(HEALTHZ_TIMEOUT_S)
    if bad and len(ages) == failures:
        raise Refusal("healthz-summary: every answered /metrics scrape for phase %s in %s was "
                      "unusable (%s), so the heartbeat age was never read"
                      % (phase, d, "; ".join(sorted(set(bad)))))
    lines.append("HEARTBEAT phase=%s n=%d failures=%d bad=%d max_s=%.3f"
                 % (phase, len(scrapes), failures, len(bad), max(ages)))
    # A warning, not a refusal: an arm on a server older than these gauges
    # still runs, but a rename that empties every scrape is not silent.
    usable = [r for r in scrapes if r.get("ok")]
    empty = sum(1 for r in usable if not r.get("memory"))
    if empty:
        sys.stderr.write("kit: healthz-summary: warning: %d of %d usable /metrics scrapes for "
                         "phase %s in %s carried none of %s\n"
                         % (empty, len(usable), phase, d, ", ".join(MEMORY_METRICS)))
    return lines


def healthz_check(path, phases):
    """The results.json healthz block from run.sh's healthz.txt, which must
    hold each phase's HEALTHZ line per port and its HEARTBEAT line exactly
    once, and nothing else."""
    want = {}
    for phase in phases:
        for port in HEALTHZ_PORTS:
            want[("HEALTHZ", phase, port)] = 0
        want[("HEARTBEAT", phase, None)] = 0
    block = dict((p, {"ports": {}, "heartbeat_max_s": None, "heartbeat_n": None,
                      "heartbeat_failures": None, "heartbeat_bad": None}) for p in phases)
    try:
        lines = open(path).read().splitlines()
    except OSError as e:
        raise Refusal("healthz-check: cannot read %s: %s" % (path, e))
    for n, line in enumerate(lines, 1):
        m, h = HEALTHZ_LINE_RE.match(line), HEARTBEAT_LINE_RE.match(line)
        key = ("HEALTHZ", m.group(1), m.group(2)) if m else (
            ("HEARTBEAT", h.group(1), None) if h else None)
        if key not in want:
            raise Refusal("healthz-check: %s:%d is not an expected line: %r" % (path, n, line))
        count, failures = (int(m.group(3)), int(m.group(7))) if m else (
            int(h.group(2)), int(h.group(3)))
        if count < 1 or failures > count:
            raise Refusal("healthz-check: %s:%d counts %d samples and %d failures; it needs at "
                          "least one sample and no more failures than samples"
                          % (path, n, count, failures))
        bad = int(h.group(4)) if h else 0
        if failures + bad > count:
            raise Refusal("healthz-check: %s:%d counts %d scrapes, %d failures and %d bad; it "
                          "needs no more failures and bad scrapes together than scrapes"
                          % (path, n, count, failures, bad))
        if bad and failures + bad == count:
            raise Refusal("healthz-check: %s:%d counts %d bad scrapes and no usable one; every "
                          "answered scrape was unusable, so the heartbeat age was never read"
                          % (path, n, bad))
        want[key] += 1
        if m:
            block[key[1]]["ports"][key[2]] = {
                "n": count, "p50_ms": float(m.group(4)), "p99_ms": float(m.group(5)),
                "max_ms": float(m.group(6)), "failures": failures}
        else:
            block[key[1]].update(heartbeat_n=count, heartbeat_failures=failures,
                                 heartbeat_bad=bad, heartbeat_max_s=float(h.group(5)))
    wrong = ["%s phase=%s%s %d times" % (k[0], k[1], " port=%s" % k[2] if k[2] else "", c)
             for k, c in sorted(want.items(), key=lambda kv: str(kv[0])) if c != 1]
    if wrong:
        raise Refusal("healthz-check: %s must hold each line exactly once; it holds %s"
                      % (path, "; ".join(wrong)))
    return {"interval_ms": HEALTHZ_INTERVAL_S * 1000.0,
            "metrics_interval_ms": METRICS_INTERVAL_S * 1000.0,
            "timeout_ms": HEALTHZ_TIMEOUT_S * 1000.0, "phases": block}


def _phase_order(phase):
    m = re.match(r"^concurrent-([0-9]+)$", phase)
    return (1, int(m.group(1)), phase) if m else (0, 0, phase)


def healthz_absolute(ab, report):
    out = []
    hz = report.get("healthz")
    for port in HEALTHZ_PORTS:
        bound = ab.get("healthz_p99_ms_max_" + port)
        if bound is None or _placeholder(bound):
            continue
        label = "healthz %s p99_ms <= healthz_p99_ms_max_%s" % ("%s", port)
        if not hz:
            out.append(_row("absolute", label % "(no phase)", None, bound, "inconclusive",
                            "no healthz samples"))
            continue
        for phase in sorted(hz["phases"], key=_phase_order):
            v = hz["phases"][phase]["ports"][port]["p99_ms"]
            out.append(_row("absolute", label % phase, v, bound, "pass" if v <= bound else "miss"))
    return out


def healthz_relative(rel, a, b):
    out = []
    ha, hb = a.get("healthz"), b.get("healthz")
    for port in HEALTHZ_PORTS:
        r = rel.get("healthz_p99_max_ratio_" + port)
        if r is None or _placeholder(r):
            continue
        label = "healthz %s port %s p99_ms <= %g x A" % ("%s", port, r)
        both = sorted(set(ha["phases"]) & set(hb["phases"]), key=_phase_order) \
            if ha and hb else []
        if not both:
            out.append(_row("relative", label % "(no phase)", None, None, "inconclusive",
                            "no phase sampled on both arms"))
            continue
        for phase in both:
            va = ha["phases"][phase]["ports"][port]["p99_ms"]
            vb = hb["phases"][phase]["ports"][port]["p99_ms"]
            bound = r * va
            out.append(_row("relative", label % phase, vb, bound,
                            "pass" if vb <= bound else "miss"))
    return out


# ADR-1170's gap between the allocator's allocated bytes and the unique
# tracked bytes (decisions 3 and 4), each term a series as metrics.jsonl
# records it and its sign in the gap. A cache term names a family in
# GAP_CACHES, read by _cache_ram.
GAP_TERMS = (
    ('ravel_process_allocator_bytes{mode="all",allocator="jemalloc",stat="allocated"}', 1),
    ("fetch", -1),
    ("catalog", -1),
    ('ravel_memory_reserved_bytes{mode="all",component="sql"}', -1),
    ('ravel_memory_reserved_bytes{mode="all",component="fetch"}', -1),
    ('ravel_memory_handoff_overlap_bytes{mode="all"}', 1),
)
GAP_CACHES = ("fetch", "catalog")
GAP_KEYS = ("adr1170_gap_max_bytes", "adr1170_gap_share_above_max")


def _cache_ram(memory, cache):
    """The RAM-resident bytes of cache family CACHE, else None. A server
    with a disk tier (--cache-dir) labels the RAM sample tier="ram" and emits
    no untiered one; a family with only a disk tier holds no RAM bytes, and
    disk-tier bytes are not heap, so they never enter the gap."""
    base = 'ravel_cache_resident_bytes{mode="all",cache="%s"' % cache
    for series in (base + "}", base + ',tier="ram"}'):
        if _num(memory.get(series)):
            return memory[series]
    if _num(memory.get(base + ',tier="disk"}')):
        return 0.0
    return None


def scrape_gap(memory):
    """allocated - cache_fetch - cache_catalog - reserved_sql - reserved_fetch
    + handoff_overlap, or the list of GAP_TERMS terms MEMORY lacks."""
    memory = memory if isinstance(memory, dict) else {}
    values, missing = [], []
    for term, sign in GAP_TERMS:
        if term in GAP_CACHES:
            v = _cache_ram(memory, term)
            if v is None:
                missing.append('ravel_cache_resident_bytes{mode="all",cache="%s"} '
                               '(no untiered, tier="ram" or tier="disk" series)' % term)
        else:
            v = memory.get(term)
            if not _num(v):
                v = None
                missing.append(term)
        if v is not None:
            values.append(sign * v)
    if missing:
        return None, missing
    return sum(values), []


def memory_gap_rounds(tgz, rounds, bar):
    """Per concurrent round, from out/concurrent/round-N/healthz/metrics.jsonl
    in the evidence tarball TGZ: the gated ok scrapes, the max gap and the
    scrapes whose gap is above BAR, with the unanswered, ungated and bad
    scrapes skipped and counted, as healthz_summary does. An ungated scrape
    falls outside the timed window, and a bad one (answered with a non-200 or
    a transport error) carries no gauges. An ok scrape missing a GAP_TERMS
    series, and a round with no gated ok scrape, are refusals."""
    if not rounds:
        raise Refusal("adr1170 gap: %s records no concurrent round" % tgz)
    out = []
    try:
        t = tarfile.open(tgz)
    except (OSError, tarfile.TarError) as e:
        raise Refusal("adr1170 gap: cannot read %s: %s" % (tgz, e))
    with t:
        for n in rounds:
            name = "out/concurrent/round-%d/healthz/metrics.jsonl" % n
            try:
                f = t.extractfile(name)
                lines = f.read().decode("utf-8").splitlines() if f else None
            except (OSError, KeyError, tarfile.TarError) as e:
                raise Refusal("adr1170 gap: round %d: cannot read %s from %s: %s"
                              % (n, name, tgz, e))
            if lines is None:
                raise Refusal("adr1170 gap: round %d: %s holds no %s" % (n, tgz, name))
            gaps, unanswered, ungated, bad = [], 0, 0, 0
            for i, line in enumerate(lines, 1):
                try:
                    rec = json.loads(line)
                except ValueError as e:
                    raise Refusal("adr1170 gap: round %d: %s:%s:%d is not a JSON line: %s"
                                  % (n, tgz, name, i, e))
                if isinstance(rec, dict) and rec.get("gated") is not True:
                    ungated += 1
                    continue
                if not isinstance(rec, dict) or rec.get("answered") is not True:
                    unanswered += 1
                    continue
                if rec.get("ok") is not True:
                    bad += 1
                    continue
                gap, missing = scrape_gap(rec.get("memory"))
                if missing:
                    raise Refusal("adr1170 gap: round %d: %s:%s:%d is an ok scrape "
                                  "missing %s" % (n, tgz, name, i, ", ".join(missing)))
                gaps.append(gap)
            if not gaps:
                raise Refusal("adr1170 gap: round %d: %s:%s holds no ok gated scrape (%s)"
                              % (n, tgz, name, _skipped(unanswered, ungated, bad)))
            above = sum(1 for g in gaps if g > bar)
            out.append({"round": n, "scrapes": len(gaps), "unanswered": unanswered,
                        "ungated": ungated, "bad": bad, "max_gap": max(gaps), "above": above,
                        "share": above / len(gaps)})
    return out


def _gap_pool(rounds):
    scrapes = sum(r["scrapes"] for r in rounds)
    above = sum(r["above"] for r in rounds)
    return {"round": None, "scrapes": scrapes, "above": above, "share": above / scrapes,
            "unanswered": sum(r["unanswered"] for r in rounds),
            "ungated": sum(r["ungated"] for r in rounds),
            "bad": sum(r["bad"] for r in rounds),
            "max_gap": max(r["max_gap"] for r in rounds)}


def _skipped(unanswered, ungated, bad):
    return "%d unanswered, %d ungated, %d bad skipped" % (unanswered, ungated, bad)


def _gap_text(fig, bar):
    return "%.1f %% of %d scrapes > %d (max %s; %s)" % (
        fig["share"] * 100.0, fig["scrapes"], bar, "{:,.0f}".format(fig["max_gap"]),
        _skipped(fig["unanswered"], fig["ungated"], fig["bad"]))


def _gap_absent(report, phases):
    if report.get("concurrent") is None:
        return "no concurrent phase ran"
    if "concurrent" not in phases:
        return "run.toml's healthz_phases leaves concurrent out"
    return ""


def memory_gap(path, a_json, a_tgz, b_json, b_tgz, run_toml):
    """ADR-1170's memory gap band, judged on arm B's concurrent rounds only: per
    round and pooled, the share of gated ok scrapes whose gap is above
    adr1170_gap_max_bytes at most adr1170_gap_share_above_max. Arm A's figures
    are printed as context and never judged; a refusal on A is printed, not
    raised. Returns the rows judged."""
    a, b = validate(a_json), validate(b_json)
    sections = b["provenance"]["band_sections"]
    if "relative" not in sections:
        return []
    rel = check_bands(lane_of(path), path, sections)["relative"]
    unset = [k for k in GAP_KEYS if rel.get(k) is None or _placeholder(rel.get(k))]
    if unset:
        print("adr1170 gap: skipped, %s missing or a placeholder in %s"
              % (" and ".join(unset), path))
        return []
    bar, share_max = rel["adr1170_gap_max_bytes"], rel["adr1170_gap_share_above_max"]
    phases = healthz_phases(check_run(run_toml))
    label = "adr1170 gap %s share > " + "%d" % bar
    absent = _gap_absent(a, phases)
    if absent:
        print("adr1170 gap A (context, not judged): %s" % absent)
    else:
        try:
            rounds = memory_gap_rounds(a_tgz, [rd["round"] for rd in a["concurrent"]["rounds"]],
                                       bar)
            for fig in rounds + [_gap_pool(rounds)]:
                print("adr1170 gap A %s (context, not judged): %s"
                      % ("round %d" % fig["round"] if fig["round"] is not None else "pooled",
                         _gap_text(fig, bar)))
        except Refusal as e:
            print("adr1170 gap A (context, not judged): unreadable: %s" % e)
    absent = _gap_absent(b, phases)
    if absent:
        return [_row("relative", label % "(no round)", None, share_max, "inconclusive", absent)]
    rounds = memory_gap_rounds(b_tgz, [rd["round"] for rd in b["concurrent"]["rounds"]], bar)
    out = []
    for fig in rounds + [_gap_pool(rounds)]:
        name = "round %d" % fig["round"] if fig["round"] is not None else "pooled"
        out.append(_row("relative", label % name, fig["share"], share_max,
                        "pass" if fig["share"] <= share_max else "miss", _gap_text(fig, bar)))
    return out


def fold_perf(lines):
    """`perf script` output folded to one `comm;outer;...;leaf count` line per
    distinct stack, the form flamegraph.pl and inferno read."""
    counts, state = {}, {"comm": None, "frames": []}

    def flush():
        if state["comm"] is not None:
            key = ";".join([state["comm"]] + state["frames"][::-1])
            counts[key] = counts.get(key, 0) + 1
        state["comm"], state["frames"] = None, []

    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip():
            flush()
        elif line[0] in " \t":
            parts = line.split(None, 1)
            sym = parts[1] if len(parts) > 1 else "[unknown]"
            sym = re.sub(r"\s+\([^)]*\)$", "", sym)
            sym = re.sub(r"\+0x[0-9a-f]+$", "", sym)
            state["frames"].append(sym.replace(";", ":") or "[unknown]")
        else:
            flush()
            m = re.match(r"^\s*(.+?)\s+[0-9]+(?:/[0-9]+)?\s", line)
            state["comm"] = (m.group(1) if m else line.split(None, 1)[0]).replace(" ", "_")
    flush()
    return ["%s %d" % (k, v) for k, v in sorted(counts.items())]


def _concurrent_errors(tgz):
    """Each statement's failed concurrent calls (not a 2xx answer with a
    completed transfer, as _ok_2xx judges), from the arm's evidence tarball,
    and the total calls read."""
    try:
        with tarfile.open(tgz) as t:
            f = t.extractfile("out/concurrent/calls.jsonl")
            lines = f.read().decode("utf-8").splitlines() if f else None
    except (OSError, KeyError, tarfile.TarError) as e:
        raise Refusal("cannot read out/concurrent/calls.jsonl from %s: %s" % (tgz, e))
    if lines is None:
        raise Refusal("%s holds no out/concurrent/calls.jsonl" % tgz)
    errors = {}
    for n, line in enumerate(lines, 1):
        try:
            c = json.loads(line)
        except ValueError as e:
            raise Refusal("%s:out/concurrent/calls.jsonl:%d is not a JSON line: %s" % (tgz, n, e))
        if not isinstance(c, dict) or "statement" not in c:
            raise Refusal("%s:out/concurrent/calls.jsonl:%d is not a call record" % (tgz, n))
        if not _ok_2xx(c):
            errors[c["statement"]] = errors.get(c["statement"], 0) + 1
    return errors, len(lines)


def _fmt(v):
    return "null" if v is None else ("%.3f" % v if isinstance(v, float) else str(v))


def concurrent_detail(bands_dir, a_json, a_tgz, b_json, b_tgz, run_toml=None):
    """Each arm's per-round QPS and error ratio beside its pooled figures, and
    on a missed concurrent error band, the missing arm's errors by statement."""
    declared = declared_flags(run_toml)
    arms = (("A", validate(a_json), a_tgz), ("B", validate(b_json), b_tgz))
    for arm, r, _ in arms:
        conc = r["concurrent"]
        if conc is None:
            print("concurrent %s: no concurrent phase ran" % arm)
            continue
        for rd in conc["rounds"]:
            print("concurrent %s round %d: qps %s error_ratio %s (ok %d, err %d, wrong_answer %d, "
                  "wall %.1f s of %s s)" % (arm, rd["round"], _fmt(rd["qps"]),
                                            _fmt(rd["error_ratio"]), rd["ok"], rd["err"],
                                            rd["wrong_answer"], rd["wall_s"], rd["duration_s"]))
            if rd.get("overran"):
                print("concurrent %s round %d overran: wall %.1f s against a nominal %s s, more "
                      "than %d%% over; its qps counts calls that finished after the deadline "
                      "over the nominal duration, so it reads high"
                      % (arm, rd["round"], rd["overran"]["wall_s"], rd["overran"]["duration_s"],
                         round(OVERRUN_FRACTION * 100)))
        p = conc["pooled"]
        print("concurrent %s pooled: qps_pooled %s error_ratio_weighted %s "
              "error_ratio_mean_of_rounds %s (ok %d, err %d over %d rounds; the bands judge "
              "error_ratio_weighted)" % (arm, _fmt(p["qps_pooled"]), _fmt(p["error_ratio_weighted"]),
                                         _fmt(p["error_ratio_mean_of_rounds"]), p["ok"], p["err"],
                                         p["rounds"]))
    missed = set()
    for arm, r, _ in arms:
        if any(b["verdict"] == "miss" and b["band"].startswith("concurrent error")
               for b in r.get("bands") or []):
            missed.add(arm)
    a, b = arms[0][1], arms[1][1]
    for name in sorted(os.listdir(bands_dir)):
        if not name.endswith(".toml"):
            continue
        rows = relative_bands(os.path.join(bands_dir, name), a, b, declared)
        if any(row["verdict"] == "miss" and row["band"].startswith("concurrent error")
               for row in rows):
            missed.add("B")
    for arm, r, tgz in arms:
        if arm not in missed:
            continue
        errors, calls = _concurrent_errors(tgz)
        total = sum(errors.values())
        print("concurrent %s errors by statement (%d errors of %d calls):" % (arm, total, calls))
        for sid, n in sorted(errors.items(), key=lambda kv: (-kv[1], kv[0])):
            print("  %-40s %d / %d" % (sid, n, total))


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


FETCH_POLICY_RE = re.compile(r' logs fetch policy resolved policy="([^"]*)" '
                             r'policy_source="([^"]*)" profile=(\S+)')


def fetch_policy(logs):
    """The logs fetch policy, its source and cost profile, from each server
    log's one `logs fetch policy resolved` line; every log must carry exactly
    one and all must agree."""
    ansi = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
    first = None
    for log in logs:
        found = [m.groups() for m in (FETCH_POLICY_RE.search(ansi.sub("", l))
                                      for l in open(log, errors="replace")) if m]
        if len(found) != 1:
            raise Refusal("server log %s carries the fetch policy line %d times, not once"
                          % (log, len(found)))
        if first is None:
            first = found[0]
        elif found[0] != first:
            raise Refusal("server log %s resolved fetch policy %r, the first resolved %r"
                          % (log, found[0], first))
    if first is None:
        raise Refusal("fetch-policy: no server log")
    return {"policy": first[0], "policy_source": first[1], "profile": first[2],
            "logs": len(logs)}


def server_settings(logs):
    """The settings every benchmark server start resolved. Each start must
    resolve every stamped setting exactly once, and each restart the same
    values, or the statements did not all run under one known configuration."""
    first, first_log = None, None
    for log in logs:
        settings, unreadable = parse_settings(open(log, errors="replace").read())
        if unreadable:
            raise Refusal("server log %s: unreadable setting line %r" % (log, unreadable[0]))
        for name in STAMPED_SETTINGS:
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
    return dict((k, v[0]) for k, v in first.items())


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
    answered = dict((int(c["try"]), float(c["duration_s"]) * 1000.0) for c in calls
                    if c.get("outcome") == "ok")
    if len(calls) != 3 or bad:
        first = bad[0] if bad else None
        if first:
            error = "%s HTTP %s %s: %s" % (first.get("outcome"), first.get("http_status"),
                                          first.get("refusal_class"), first.get("error"))
            run = int(first["try"]) - 1
        else:
            error, run = "%d of 3 tries recorded" % len(calls), len(calls)
        return None, {"id": sid, "run": run, "error": error, "answer": answer,
                      "tries_ms": [answered.get(t) for t in (1, 2, 3)]}
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
    binary, tenant, phases_json. Optional: concurrent and q29_probe, each the
    phase's summary.json, when that phase ran, and healthz, healthz-check's
    block."""
    kv = dict(a.split("=", 1) for a in args)
    for key in ("out", "calls", "index", "server_logs", "bands", "sections", "provenance",
                "arm", "sha", "binary", "tenant", "phases_json"):
        if key not in kv:
            raise Refusal("assemble: missing %s=" % key)
    # An empty bands= and sections= is a run with no lanes (cloud gcp): no
    # band file is judged on the box, and its pre-registration is judged by
    # kit.py prereg after fetch.
    if kv["bands"] == "" and kv["sections"] == "":
        sections = []
    else:
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
    for item in (kv["bands"].split(",") if sections else []):
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
    calls, errors = pooled_errors(phases)
    blocks = {}
    for key in ("concurrent", "q29_probe", "healthz"):
        blocks[key] = json.load(open(kv[key])) if kv.get(key) else None
    report = {
        "provenance": provenance,
        "timing_definition": TIMING_DEFINITION,
        "entries": entries,
        "skipped": [],
        "failed": failed,
        "totals": totals,
        "error_pooled": {"calls": calls, "errors": errors,
                         "ratio": (errors / calls) if calls else None,
                         "phases": list(POOLED_PHASES)},
        "concurrent": blocks["concurrent"],
        "q29_probe": blocks["q29_probe"],
        "healthz": blocks["healthz"],
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
    conc = r["concurrent"]
    if conc is not None:
        if not isinstance(conc, dict) or not isinstance(conc.get("rounds"), list) \
                or not isinstance(conc.get("pooled"), dict):
            raise Refusal("%s: concurrent is neither null nor a block with rounds and pooled" % path)
        for rd in conc["rounds"]:
            miss = [k for k in CONCURRENT_ROUND_FIELDS if k not in rd]
            if miss:
                raise Refusal("%s: concurrent round %r missing %s"
                              % (path, rd.get("round"), ", ".join(miss)))
        miss = [k for k in CONCURRENT_POOLED_FIELDS if k not in conc["pooled"]]
        if miss:
            raise Refusal("%s: concurrent.pooled missing %s" % (path, ", ".join(miss)))
    probe = r["q29_probe"]
    if probe is not None:
        miss = [k for k in Q29_PROBE_FIELDS if k not in (probe if isinstance(probe, dict) else {})]
        if miss:
            raise Refusal("%s: q29_probe missing %s" % (path, ", ".join(miss)))
    hz = r.get("healthz")
    if hz is not None:
        phases = hz.get("phases") if isinstance(hz, dict) else None
        if not isinstance(phases, dict) or not all(
                isinstance(p, dict) and all(_num(((p.get("ports") or {}).get(port) or {})
                                                 .get("p99_ms")) for port in HEALTHZ_PORTS)
                for p in phases.values()):
            raise Refusal("%s: healthz is neither null nor a block with each phase's p99_ms "
                          "on ports %s" % (path, " and ".join(HEALTHZ_PORTS)))
    for b in r["bands"]:
        if b.get("verdict") not in ("pass", "miss", "inconclusive"):
            raise Refusal("%s: band %r has verdict %r" % (path, b.get("band"), b.get("verdict")))
    return r


# --- GCP path: names, store listing, upstream result, pre-registration -----

def gcp_names(run_id, store_bucket_prefix):
    """The run's instance name, store bucket and label value, each derived
    from the run id alone, so launch-gcp.sh and teardown-gcp.sh name the same
    resources without a record of them."""
    m = RUN_ID_RE.match(run_id)
    if not m:
        raise Refusal("gcp-names: %r is not a kit run id" % run_id)
    if not STORE_BUCKET_PREFIX_RE.match(store_bucket_prefix):
        raise Refusal("gcp-names: %r is not a store bucket prefix" % store_bucket_prefix)
    tail = m.group(1).lower()
    names = ("ravel-clickbench-" + tail, store_bucket_prefix + "-" + tail, run_id.lower())
    for n in names:
        if len(n) > 63:
            raise Refusal("gcp-names: %s is longer than 63 characters" % n)
    return names


def _strict_json(path):
    """JSON from PATH, refusing an object that names a key twice: a figure
    emitted twice must not be read as whichever came last."""
    def pairs(items):
        seen = {}
        for k, v in items:
            if k in seen:
                raise Refusal("%s: key %r appears twice in one object" % (path, k))
            seen[k] = v
        return seen
    try:
        with open(path) as f:
            return json.load(f, object_pairs_hook=pairs)
    except (OSError, ValueError) as e:
        raise Refusal("%s: not readable JSON: %s" % (path, e))


def gcs_next_page(path):
    """The nextPageToken of one GCS JSON API objects.list page, URL-encoded
    for the next request; empty on the last page."""
    page = _strict_json(path)
    if not isinstance(page, dict) or "error" in page:
        raise Refusal("%s: not an objects.list page: %s" % (path, str(page)[:300]))
    token = page.get("nextPageToken")
    if token is None:
        return ""
    if not isinstance(token, str) or not token:
        raise Refusal("%s: nextPageToken %r is not a token" % (path, token))
    from urllib.parse import quote
    return quote(token, safe="")


def _key_class(name):
    """(scope, class) of one store key: ("t/<hash>", "<signal>/<kind>") for a
    tenant key such as t/<hash>/l/l0/..., ("sys", "sys/<x>") for a sys/ key,
    ("other", <first segment>) for anything else."""
    parts = name.split("/")
    if parts[0] == "t" and len(parts) >= 3:
        return "t/" + parts[1], "/".join(parts[2:4])
    if parts[0] == "sys":
        return "sys", "/".join(parts[:2])
    return "other", parts[0]


def footprint(paths):
    """Objects and bytes in Ravel's store bucket from its objects.list pages,
    split by key class: every tenant prefix t/<hash>/ (its bytes are the
    tenant's whole footprint, ClickBench's data_size basis; its l/l0 class is
    the data objects), sys/ (qualification records and probes) and anything
    else. A key listed twice is a paging error and refused."""
    seen = set()
    out = {"basis": "every key in the store bucket, by objects.list; a tenant's bytes are all "
                    "of t/<hash>/, its l/l0 class is the data objects",
           "objects": 0, "bytes": 0, "tenants": {}, "sys": {}, "other": {}}
    for path in paths:
        page = _strict_json(path)
        if not isinstance(page, dict) or "error" in page:
            raise Refusal("%s: not an objects.list page: %s" % (path, str(page)[:300]))
        for item in page.get("items", []):
            name, size = item.get("name"), item.get("size")
            if not isinstance(name, str) or not re.match(r"^[0-9]+$", str(size)):
                raise Refusal("%s: item %r has no name and size" % (path, item))
            if name in seen:
                raise Refusal("%s: %s is listed twice" % (path, name))
            seen.add(name)
            size = int(size)
            out["objects"] += 1
            out["bytes"] += size
            scope, cls = _key_class(name)
            if scope.startswith("t/"):
                t = out["tenants"].setdefault(scope[2:], {"objects": 0, "bytes": 0, "classes": {}})
                t["objects"] += 1
                t["bytes"] += size
                bucket = t["classes"].setdefault(cls, {"objects": 0, "bytes": 0})
            else:
                bucket = out[scope].setdefault(cls, {"objects": 0, "bytes": 0})
            bucket["objects"] += 1
            bucket["bytes"] += size
    return out


def _one_tenant(fp, where):
    tenants = (fp or {}).get("tenants")
    if not isinstance(tenants, dict) or len(tenants) != 1:
        raise Refusal("%s: store_footprint holds %s tenant prefixes, not one (two runs in one "
                      "bucket, or no load)" % (where, len(tenants) if isinstance(tenants, dict)
                                               else "no"))
    return list(tenants.values())[0]


LOAD_WINDOW_STEPS = ("load", "compact", "fold", "declare")
STAMP_RE = re.compile(r"^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z) (.*)$")


def _stamp_seconds(text):
    return calendar.timegm(time.strptime(text, "%Y-%m-%dT%H:%M:%SZ"))


def progress_stamps(path, line):
    """The UTC times of every progress.log line whose text is LINE."""
    found = []
    try:
        lines = open(path).read().splitlines()
    except OSError as e:
        raise Refusal("cannot read %s: %s" % (path, e))
    for raw in lines:
        m = STAMP_RE.match(raw)
        if m and m.group(2) == line:
            found.append(m.group(1))
    return found


def _progress_once(path, line):
    found = progress_stamps(path, line)
    if len(found) != 1:
        raise Refusal("%s carries `%s` %d times, not once" % (path, line, len(found)))
    return found[0]


def load_window(path):
    """Seconds each of the load window's steps took, from progress.log's
    start and done stamps (each exactly once), and their sum: the figure the
    upstream result's load_time carries. The window is the ravel-cli load,
    compaction, fold and declaration, what the ClickBench entry's ./load
    times; the store listings between them are not in it."""
    steps = {}
    for step in LOAD_WINDOW_STEPS:
        start = _stamp_seconds(_progress_once(path, "step=%s state=start" % step))
        done = _stamp_seconds(_progress_once(path, "step=%s state=done" % step))
        if done < start:
            raise Refusal("%s: step %s is done before it starts" % (path, step))
        steps[step] = done - start
    return {"steps": steps, "total_s": sum(steps.values())}


def _statement_number(sid):
    m = re.match(r"^q([0-9]{2})_", str(sid))
    if not m:
        raise Refusal("statement id %r carries no qNN_ number" % sid)
    return int(m.group(1))


def _round3(v):
    return None if v is None else round(float(v), 3)


def upstream_result(results_path, progress_path, expected=43):
    """results.json and progress.log as one ClickBench results file: 43
    [cold, hot, hot] triples in seconds (null for a try that did not
    answer), load_time from load_window, data_size the tenant's bytes in the
    store footprint, and concurrent_qps and concurrent_error_ratio the
    concurrent phase's pooled figures (the mean of the rounds' QPS and all
    rounds' failed calls over all their calls)."""
    _strict_json(results_path)
    r = validate(results_path)
    p = r["provenance"]
    rows = {}
    for e in r["entries"] + r["failed"]:
        n = _statement_number(e["id"])
        if n in rows:
            raise Refusal("%s: statement %d appears twice" % (results_path, n))
        tries = e.get("tries_ms")
        if not isinstance(tries, list) or len(tries) != 3:
            raise Refusal("%s: statement %s has no three tries_ms" % (results_path, e["id"]))
        rows[n] = [_round3(t / 1000.0) if t is not None else None for t in tries]
    if sorted(rows) != list(range(1, expected + 1)):
        raise Refusal("%s: statements %s, not q01 to q%02d"
                      % (results_path, sorted(rows), expected))
    conc = r.get("concurrent")
    if not isinstance(conc, dict):
        raise Refusal("%s: no concurrent block: the concurrent phase did not run" % results_path)
    pooled = conc["pooled"]
    tenant = _one_tenant(p.get("store_footprint"), results_path)
    machine = (p.get("hardware") or {}).get("instance_type")
    if not machine or not MACHINE_RE.match(str(machine)):
        raise Refusal("%s: hardware.instance_type %r is not a machine" % (results_path, machine))
    started = _progress_once(progress_path, "step=phase-benchmark state=start")
    return {
        "system": "Ravel",
        "date": started[:10],
        "machine": machine,
        "cluster_size": 1,
        "proprietary": "no",
        "hardware": "cpu",
        "tuned": "no",
        "tags": ["Rust", "column-oriented", "GCS"],
        "load_time": int(round(load_window(progress_path)["total_s"])),
        "data_size": tenant["bytes"],
        "concurrent_qps": _round3(pooled["qps_pooled"]),
        "concurrent_error_ratio": _round3(pooled["error_ratio_weighted"]),
        "result": [rows[n] for n in range(1, expected + 1)],
    }


class _Missing(Exception):
    pass


def _dig(d, *keys):
    for k in keys:
        if not isinstance(d, dict) or k not in d or d[k] is None:
            raise _Missing(".".join(str(x) for x in keys))
        d = d[k]
    return d


def _prereg_figures(r, up, store_bucket):
    """name -> function giving the measured value; raising _Missing when the
    run did not produce it."""
    p = r["provenance"]

    def tenant_class(cls, field):
        def get():
            t = _one_tenant(_dig(p, "store_footprint"), "results.json")
            return _dig(t, "classes", cls, field)
        return get

    def nulls():
        return sum(1 for row in _dig(up, "result") for v in row if v is None)

    def minus(a, b):
        return lambda: a() - b()

    written = lambda: _dig(p, "fold", "objects_written_by_load")
    return {
        "rows_written": lambda: _dig(p, "row_count", "load_rows_written"),
        "count_star": lambda: _dig(p, "row_count", "count_star"),
        "statements_answered": lambda: _dig(r, "totals", "answered"),
        "null_tries": nulls,
        "benchmark_error_ratio": lambda: _dig(r, "error_pooled", "ratio"),
        "data_size_bytes": lambda: _dig(up, "data_size"),
        "l0_objects": tenant_class("l/l0", "objects"),
        "l0_objects_minus_objects_written": minus(tenant_class("l/l0", "objects"), written),
        "fold_entries_minus_objects_written": minus(lambda: _dig(p, "fold", "entry_count"),
                                                    written),
        "declared_columns": lambda: _dig(p, "declared_columns", "count"),
        "load_time_s": lambda: _dig(up, "load_time"),
        "cold_sum_s": lambda: _dig(r, "totals", "cold_sum_s"),
        "hot_sum_s": lambda: _dig(r, "totals", "hot_sum_s"),
        "concurrent_qps": lambda: _dig(r, "concurrent", "pooled", "qps_pooled"),
        "concurrent_error_ratio": lambda: _dig(r, "concurrent", "pooled",
                                               "error_ratio_weighted"),
        "store": lambda: _dig(p, "backend", "store"),
        "store_identity": lambda: _dig(p, "backend", "identity"),
        "fold_precondition": lambda: _dig(p, "fold", "precondition"),
        "fetch_policy": lambda: _dig(p, "fetch_policy", "policy"),
        "fetch_policy_source": lambda: _dig(p, "fetch_policy", "policy_source"),
        "binary_source": lambda: _dig(p, "release", "source"),
        "release_tag": lambda: _dig(p, "release", "tag"),
        "ravel_server_sha256": lambda: _dig(p, "release", "server_sha256"),
        "ravel_cli_sha256": lambda: _dig(p, "release", "cli_sha256"),
        "machine": lambda: _dig(p, "hardware", "instance_type"),
    }


PREREG_FIGURES = (
    "rows_written", "count_star", "statements_answered", "null_tries", "benchmark_error_ratio",
    "data_size_bytes", "l0_objects", "l0_objects_minus_objects_written",
    "fold_entries_minus_objects_written", "declared_columns", "load_time_s", "cold_sum_s",
    "hot_sum_s", "concurrent_qps", "concurrent_error_ratio", "store", "store_identity",
    "fold_precondition", "fetch_policy", "fetch_policy_source", "binary_source", "release_tag",
    "ravel_server_sha256", "ravel_cli_sha256", "machine",
)


def check_prereg(path):
    """A pre-registration file: one [figures] table naming each of
    PREREG_FIGURES once, as an exact value or a [low, high] band."""
    data = parse_toml(path)
    unknown = [k for k in data if k != "figures"]
    if unknown:
        raise Refusal("%s: unknown key(s) or table(s) %s; every band sits under [figures]"
                      % (path, ", ".join(sorted(unknown))))
    bands = data.get("figures")
    if not isinstance(bands, dict):
        raise Refusal("%s: no [figures] table" % path)
    extra = sorted(set(bands) - set(PREREG_FIGURES))
    absent = [k for k in PREREG_FIGURES if k not in bands]
    if extra:
        raise Refusal("%s: unknown figure(s) %s" % (path, ", ".join(extra)))
    if absent:
        raise Refusal("%s: not pre-registered: %s" % (path, ", ".join(absent)))
    for name, band in bands.items():
        if isinstance(band, list):
            if len(band) != 2 or not all(_num(b) for b in band) or band[0] > band[1]:
                raise Refusal("%s: %s band %r is not [low, high]" % (path, name, band))
        elif isinstance(band, bool) or not isinstance(band, (int, float, str)):
            raise Refusal("%s: %s is %r, neither a value nor a [low, high] band"
                          % (path, name, band))
    return bands


def prereg(prereg_path, results_path, progress_path, upstream_path, store_bucket):
    """Judge a run against its pre-registration. PREREG's [figures] table
    names every figure _prereg_figures knows, each either an exact value
    (number or string; a string may say {store_bucket}) or a [low, high]
    band, both ends inclusive. Returns (rows, unjudged): a figure the run did
    not produce, or produced twice, is unjudged, never a pass."""
    bands = check_prereg(prereg_path)
    up = _strict_json(upstream_path)
    _strict_json(results_path)
    r = validate(results_path)
    figures = _prereg_figures(r, up, store_bucket)
    if tuple(figures) != PREREG_FIGURES:
        raise Refusal("prereg: the judge's figures and PREREG_FIGURES differ")
    # A figure read from a log line, not from a JSON field: each step's start
    # and done stamps must appear exactly once.
    window_error = None
    try:
        load_window(progress_path)
    except Refusal as e:
        window_error = str(e)
    rows, unjudged = [], []
    for name in PREREG_FIGURES:
        band = bands[name]
        if isinstance(band, str):
            band = band.replace("{store_bucket}", store_bucket)
        try:
            if name == "load_time_s" and window_error:
                raise _Missing(window_error)
            value = figures[name]()
        except (_Missing, Refusal) as e:
            unjudged.append(name)
            rows.append(_row("prereg", name, None, band, "unjudged", "missing: %s" % e))
            continue
        if isinstance(band, list):
            ok = _num(value) and band[0] <= value <= band[1]
            bound = "%s to %s" % tuple(band)
        else:
            ok = value == band
            bound = band
        rows.append(_row("prereg", name, value, bound, "pass" if ok else "miss"))
    return rows, unjudged


PUBLISHED_OPTIONS = ("--published-tries", "--arm-o", "--qps-windows", "--new-arm",
                     "--figures")


def figures_arg(value):
    """The kinds named by a --figures VALUE, in FIGURE_KINDS order; None when
    it is empty, names an unknown kind or names one twice."""
    kinds = value.split(",")
    if any(k not in FIGURE_KINDS for k in kinds) or len(set(kinds)) != len(kinds):
        return None
    return tuple(k for k in FIGURE_KINDS if k in kinds)


def published_args(args):
    """published's four positional arguments and its options, each given at
    most once with a value; None on bad usage, including a --figures value
    figures_arg refuses, --arm-o without
    --published-tries and --new-arm without --qps-windows, each of which
    would otherwise be read by nothing."""
    pos, opts = [], {}
    i = 0
    while i < len(args):
        if args[i].startswith("--"):
            if args[i] not in PUBLISHED_OPTIONS or args[i] in opts or i + 1 >= len(args):
                return None
            opts[args[i]] = args[i + 1]
            i += 2
        else:
            pos.append(args[i])
            i += 1
    if len(pos) != 4:
        return None
    if "--figures" in opts and figures_arg(opts["--figures"]) is None:
        return None
    if "--arm-o" in opts and "--published-tries" not in opts:
        return None
    if "--new-arm" in opts and "--qps-windows" not in opts:
        return None
    return pos, opts


def _results(path):
    """A results file in ClickBench's format, or a validated kit
    results.json."""
    try:
        report = json.load(open(path))
    except (OSError, ValueError) as e:
        raise Refusal("%s: not readable JSON: %s" % (path, e))
    if not isinstance(report, dict) or "result" not in report:
        report = validate(path)
    return report


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 64
    cmd, args = argv[1], argv[2:]
    try:
        if cmd == "get" and len(args) == 2:
            v = lookup(parse_toml(args[0]), args[1])
            if v is None:
                print("kit: %s: no key %s" % (args[0], args[1]), file=sys.stderr)
                return 3
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
        elif cmd == "resolved-run" and len(args) == 2:
            resolved_run(*args)
        elif cmd == "permutation" and len(args) == 3:
            print(" ".join(str(i) for i in permutation(*args)))
        elif cmd == "concurrent-answers" and len(args) == 1:
            concurrent_answers(args[0])
        elif cmd == "concurrent-summary" and len(args) == 1:
            print(json.dumps(concurrent_summary(args[0]), indent=2, sort_keys=True))
        elif cmd == "q29-summary" and len(args) == 1:
            print(json.dumps(q29_summary(args[0]), indent=2, sort_keys=True))
        elif cmd == "fold-perf" and not args:
            for line in fold_perf(sys.stdin):
                print(line)
        elif cmd == "concurrent-detail" and len(args) in (5, 6):
            concurrent_detail(*args)
        elif cmd == "comparable" and len(args) == 3:
            comparable(*args)
        elif cmd == "healthz-sampler" and len(args) == 5:
            healthz_sampler(*args)
        elif cmd == "healthz-summary" and len(args) == 2:
            for line in healthz_summary(*args):
                print(line)
        elif cmd == "healthz-check" and len(args) >= 2:
            print(json.dumps(healthz_check(args[0], args[1:]), indent=2, sort_keys=True))
        elif cmd == "assemble":
            assemble(args)
        elif cmd == "validate" and len(args) == 1:
            validate(args[0])
        elif cmd == "absolute" and len(args) == 2:
            rows = validate(args[1])["bands"]
            print_table(rows, args[0])
            return 1 if any(r["verdict"] == "miss" for r in rows) else 0
        elif cmd == "published" and published_args(args) is not None:
            (label, bands, machine, path), opts = published_args(args)
            tries = arm_o = windows = None
            if "--published-tries" in opts:
                tries = (opts["--published-tries"], _results(opts["--published-tries"]))
            if "--arm-o" in opts:
                arm_o = (opts["--arm-o"], _results(opts["--arm-o"]))
            if "--qps-windows" in opts:
                windows = (opts["--qps-windows"], opts.get("--new-arm", QPS_NEW_ARM))
            judged = figures_arg(opts.get("--figures", ",".join(FIGURE_KINDS)))
            skipped = {}
            rows = published_bands(bands, machine, _results(path), path, tries, arm_o, windows,
                                   judged, skipped)
            print_table(rows, label)
            for kind in FIGURE_KINDS:
                if kind not in judged:
                    print("SKIPPED %s: %d statements not judged (--figures %s)"
                          % (kind, skipped.get(kind, 0), ",".join(judged)))
            if any(r["verdict"] == "miss" for r in rows):
                return 1
            unjudged = [r["statement"] for r in rows if r["verdict"] == "unjudged"]
            if unjudged:
                hint = ("arm O holds no hot figure for them" if arm_o is not None
                        else "give --arm-o to judge them")
                print("kit: run incomplete: %s unjudged (unstable published reference); %s"
                      % (", ".join(unjudged), hint), file=sys.stderr)
                return EXIT_INCOMPLETE
            return 0
        elif cmd == "relative" and len(args) in (3, 4):
            a, b = validate(args[1]), validate(args[2])
            run_toml = args[3] if len(args) == 4 else None
            rows = relative_bands(args[0], a, b, declared_flags(run_toml),
                                  box_speed_enforced(run_toml))
            print_table(rows, "B vs A")
            if any(r["verdict"] == "invalid" for r in rows):
                print("kit: run INVALID: the box speed band failed, so the boxes ran at "
                      "different speeds; re-run, do not read the bands", file=sys.stderr)
                return EXIT_INVALID
            return 1 if any(r["verdict"] == "miss" for r in rows) else 0
        elif cmd == "box-speed" and len(args) == 1:
            print(json.dumps(box_speed_stamp(args[0]), sort_keys=True))
        elif cmd == "memory-gap" and len(args) == 6:
            rows = memory_gap(*args)
            print_table(rows, "B")
            return 1 if any(r["verdict"] == "miss" for r in rows) else 0
        elif cmd == "fetch-policy" and args:
            print(json.dumps(fetch_policy(args), sort_keys=True))
        elif cmd == "gcp-names" and len(args) == 2:
            for name in gcp_names(*args):
                print(name)
        elif cmd == "gcs-next-page" and len(args) == 1:
            print(gcs_next_page(args[0]))
        elif cmd == "footprint" and args:
            print(json.dumps(footprint(args), indent=2, sort_keys=True))
        elif cmd == "load-window" and len(args) == 1:
            print(json.dumps(load_window(args[0]), sort_keys=True))
        elif cmd == "upstream-result" and len(args) == 2:
            print(json.dumps(upstream_result(*args), indent=2))
        elif cmd == "check-prereg" and len(args) == 1:
            check_prereg(args[0])
        elif cmd == "prereg" and len(args) == 5:
            rows, unjudged = prereg(*args)
            print_table(rows, "A")
            if unjudged:
                print("kit: not judged, missing or emitted twice: %s" % ", ".join(unjudged),
                      file=sys.stderr)
                return 2
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
