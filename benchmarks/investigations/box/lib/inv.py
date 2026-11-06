#!/usr/bin/env python3
"""Helpers for box/run.sh (issue #3699, ADR-3692 decision 8). Stdlib only.

Subcommands:

  s3 wait|mkbucket|census   a SigV4 client for the store the run writes to,
                            read from RAVEL_S3_ENDPOINT, RAVEL_S3_BUCKET,
                            RAVEL_S3_REGION, RAVEL_S3_ACCESS_KEY and
                            RAVEL_S3_SECRET_KEY (no aws CLI needed)
  plan                      render statements.toml for one state into one
                            JSON line per call (placeholders filled from
                            truth.json)
  call                      perform one planned call against the server and
                            append its calls.jsonl line
  unsupported               append the calls.jsonl line of a statement or
                            state this build cannot run
  measure                   run a command, write its wall time, CPU seconds
                            and peak RSS as JSON
  census-listing            the census of an aws s3api list-objects-v2
                            listing (the s3 lane)
  merge-truth               fold the tail and many_parts_N corpora's truth
                            into truth.json (sub.<name>, answers <name>.*)
  inspect-stamp             one state's inspect stamps for one tenant and
                            signal: catalog, data object header, leaf,
                            typed-attr-column show, and with --ranges-keys
                            every data object's event range
  needle-blocks             storage blocks holding a needle, from rlog
                            inspect's field directory
  stamp                     write or append one stamp document
  get                       print one run.toml value for bash
  warmup                    wait for the server's startup cache warmup and
                            print what it warmed
  stride-check              the event-time check a strided load runs before
                            each batch, run over the whole Parquet file
                            before the load (reads it through pyarrow, the
                            corpus generator's own dependency; every other
                            subcommand is stdlib only)

Every failure exits non-zero with one line on stderr.
"""

import argparse
import datetime
import hashlib
import hmac
import http.client
import json
import os
import resource
import subprocess
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ROW_SET_MAX = 100
HOUR_NS = 3_600_000_000_000
# ravel_catalog::DEFAULT_MAX_INGEST_LAG_NS (crates/ravel-catalog/src/config.rs:8),
# the lag a strided load's per-batch check adds to each ingest hour's end.
MAX_INGEST_LAG_NS = 2 * HOUR_NS
NARROW_NS = 300_000_000_000


def die(msg, code=1):
    print(f"inv.py: {msg}", file=sys.stderr)
    sys.exit(code)


# --- SigV4 S3 client ---------------------------------------------------------


def s3_env():
    keys = ["RAVEL_S3_ENDPOINT", "RAVEL_S3_BUCKET", "RAVEL_S3_ACCESS_KEY", "RAVEL_S3_SECRET_KEY"]
    missing = [k for k in keys if not os.environ.get(k)]
    if missing:
        die("unset: " + " ".join(missing), 64)
    return {
        "endpoint": os.environ["RAVEL_S3_ENDPOINT"].rstrip("/"),
        "bucket": os.environ["RAVEL_S3_BUCKET"],
        "region": os.environ.get("RAVEL_S3_REGION", "us-east-1"),
        "access": os.environ["RAVEL_S3_ACCESS_KEY"],
        "secret": os.environ["RAVEL_S3_SECRET_KEY"],
    }


def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def s3_request(env, method, path, query=None, headers=None, body=b""):
    query = query or {}
    headers = dict(headers or {})
    url = urllib.parse.urlsplit(env["endpoint"])
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    day = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    headers["host"] = url.netloc
    headers["x-amz-date"] = amz_date
    headers["x-amz-content-sha256"] = payload_hash
    canonical_query = "&".join(
        f"{urllib.parse.quote(k, safe='-_.~')}={urllib.parse.quote(str(v), safe='-_.~')}"
        for k, v in sorted(query.items())
    )
    signed = sorted(h.lower() for h in headers)
    canonical_headers = "".join(f"{h}:{str(headers[h]).strip()}\n" for h in signed)
    canonical = "\n".join(
        [method, urllib.parse.quote(path, safe="/-_.~"), canonical_query, canonical_headers, ";".join(signed), payload_hash]
    )
    scope = f"{day}/{env['region']}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    k = _sign(("AWS4" + env["secret"]).encode(), day)
    k = _sign(k, env["region"])
    k = _sign(k, "s3")
    k = _sign(k, "aws4_request")
    sig = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={env['access']}/{scope}, SignedHeaders={';'.join(signed)}, Signature={sig}"
    )
    full = env["endpoint"] + urllib.parse.quote(path, safe="/-_.~") + ("?" + canonical_query if canonical_query else "")
    req = urllib.request.Request(full, data=body if method in ("PUT", "POST") else None, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _strip_ns(tag):
    return tag.split("}", 1)[-1]


def s3_list_all(env, prefix=""):
    """Every key under prefix, paginated by continuation token."""
    out = []
    token = None
    while True:
        q = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
        if token:
            q["continuation-token"] = token
        code, body = s3_request(env, "GET", f"/{env['bucket']}", q)
        if code != 200:
            die(f"list {prefix!r}: HTTP {code}: {body[:200]!r}")
        root = ET.fromstring(body)
        token = None
        truncated = False
        for el in root:
            tag = _strip_ns(el.tag)
            if tag == "Contents":
                key = size = None
                for c in el:
                    if _strip_ns(c.tag) == "Key":
                        key = c.text
                    elif _strip_ns(c.tag) == "Size":
                        size = int(c.text)
                out.append((key, size))
            elif tag == "IsTruncated":
                truncated = el.text == "true"
            elif tag == "NextContinuationToken":
                token = el.text
        if not truncated:
            return out
        if not token:
            die("list truncated without a continuation token")


SIGNAL_DIR = {"s": "spans", "l": "logs", "m": "metrics"}
# Data objects live under <signal>/l0/ and <signal>/l1/ with the .rseg suffix
# for every signal (docs/catalog-and-mvcc.md "Key layout"; ravel-commit
# keys::DATA_SUFFIX). Commit records, provenance, delete markers and the rest
# of the signal prefix are census classes, never data objects.
DATA_LEVELS = ("l0", "l1")
DATA_SUFFIX = ".rseg"


def is_data_key(rest):
    """rest: a tenant key's path after t/<tenant_hash>/."""
    return len(rest) > 2 and rest[0] in SIGNAL_DIR and rest[1] in DATA_LEVELS and rest[-1].endswith(DATA_SUFFIX)


def object_class(key):
    """The census class of a key, by its layout path (docs/catalog-and-mvcc.md):
    a signal's keys split by their level (l/l0, l/c, l/prov), the catalog by
    signal."""
    parts = key.split("/")
    if parts[0] != "t" or len(parts) < 3:
        return parts[0]
    rest = parts[2:]
    head = rest[0]
    if head in SIGNAL_DIR and len(rest) > 1:
        return f"{head}/{rest[1]}"
    if head == "catalog" and len(rest) > 2:
        return f"catalog/{rest[1]}"
    return head


def cmd_s3(args):
    env = s3_env()
    if args.op == "wait":
        deadline = time.monotonic() + args.timeout
        while True:
            try:
                code, _ = s3_request(env, "GET", "/")
                if code == 200:
                    return
            except OSError:
                pass
            if time.monotonic() > deadline:
                die(f"store at {env['endpoint']} not answering after {args.timeout}s")
            time.sleep(0.5)
    if args.op == "mkbucket":
        code, body = s3_request(
            env, "PUT", f"/{env['bucket']}", headers={"x-amz-bucket-object-lock-enabled": "true"}
        )
        if code not in (200, 409):
            die(f"create bucket {env['bucket']}: HTTP {code}: {body[:200]!r}")
        return
    if args.op == "census":
        keys = s3_list_all(env, args.prefix)
        write_keys(args.keys_out, keys)
        write_json(args.out, census_doc(env["bucket"], args.prefix, keys, load_tenants(args.tenants)))
        return
    die(f"unknown s3 op {args.op}", 64)


# The logs keyed plan probes every data object past this size (#3117), so
# row 6 expects at least one keyIndex GET for each.
LARGE_OBJECT_BYTES = 512 * 1024


def tenant_census(keys, tenant_hash):
    """One tenant's objects: classes by layout path, each signal's data objects
    (.rseg under l0/ and l1/ only) with its largest (the object the per-state
    inspect stamps read), and every catalog object's size (HEAD, snapshot
    parts, key-index leaves). A signal with keys but no data object reads
    objects 0."""
    classes, data, catalog = {}, {}, {}
    for key, size in keys:
        parts = key.split("/")
        if len(parts) < 4 or parts[0] != "t" or parts[1] != tenant_hash:
            continue
        rest = parts[2:]
        if rest[0] == "catalog":
            cls = "/".join(rest[:3]) if len(rest) > 3 else "/".join(rest[:2]) + "/HEAD"
            catalog[key] = size
        elif rest[0] in SIGNAL_DIR:
            cls = "/".join(rest[:2])
            d = data.setdefault(SIGNAL_DIR[rest[0]], {"objects": 0, "bytes": 0, "l0_objects": 0, "l1_objects": 0,
                                                      "largest": None, "largest_bytes": -1, "objects_over_512k": 0})
            if is_data_key(rest):
                d["objects"] += 1
                d["l1_objects" if rest[1] == "l1" else "l0_objects"] += 1
                d["bytes"] += size
                d["objects_over_512k"] += size > LARGE_OBJECT_BYTES
                if size > d["largest_bytes"] or (size == d["largest_bytes"] and key < d["largest"]):
                    d["largest"], d["largest_bytes"] = key, size
            elif rest[1] in DATA_LEVELS:
                cls += "/other"
        else:
            cls = rest[0]
        c = classes.setdefault(cls, {"objects": 0, "bytes": 0})
        c["objects"] += 1
        c["bytes"] += size
    return {"hash": tenant_hash, "classes": dict(sorted(classes.items())), "data": dict(sorted(data.items())),
            "catalog_objects": dict(sorted(catalog.items()))}


def census_doc(bucket, prefix, keys, tenants=None):
    classes = {}
    for key, size in keys:
        c = classes.setdefault(object_class(key), {"objects": 0, "bytes": 0})
        c["objects"] += 1
        c["bytes"] += size
    doc = {
        "verified_at": utc_now(),
        "bucket": bucket,
        "prefix": prefix,
        "objects": len(keys),
        "bytes": sum(s for _, s in keys),
        "classes": dict(sorted(classes.items())),
    }
    if tenants:
        doc["tenants"] = {name: tenant_census(keys, h) for name, h in sorted(tenants.items())}
    return doc


def load_tenants(path):
    if not path:
        return None
    with open(path) as f:
        return json.load(f)


def write_keys(path, keys):
    if path:
        with open(path, "w") as f:
            for key, size in keys:
                f.write(f"{size}\t{key}\n")


def cmd_census_listing(args):
    """The census of an `aws s3api list-objects-v2 --output json` listing (the
    s3 lane, whose credentials are the instance role's)."""
    try:
        with open(args.listing) as f:
            text = f.read()
        doc = json.loads(text) if text.strip() else {}
    except (OSError, ValueError) as e:
        die(f"census-listing: {args.listing}: {e}")
    keys = [(o["Key"], int(o["Size"])) for o in doc.get("Contents") or []]
    write_keys(args.keys_out, keys)
    write_json(args.out, census_doc(args.bucket, "", keys, load_tenants(args.tenants)))


# --- truth, inspect stamps -----------------------------------------------------


def cmd_merge_truth(args):
    """Fold the extra corpora (the mixed state's tail hour, each many_parts_N
    tenant's corpus) into truth.json: each under sub.<name>, its answers
    copied to the top level under <name>., and mixed.needle.* the sum of the
    base and tail needle counts (the mixed state holds both)."""
    with open(args.base) as f:
        truth = json.load(f)
    truth["sub"] = {}
    for spec in args.sub:
        name, sep, path = spec.partition("=")
        if not sep or not name:
            die(f"merge-truth: --sub wants NAME=PATH, got {spec!r}", 64)
        with open(path) as f:
            sub = json.load(f)
        truth["sub"][name] = sub
        for k, v in sub["answers"].items():
            truth["answers"][f"{name}.{k}"] = v
    tail = truth["sub"].get("tail")
    if tail:
        for k in [k for k in truth["answers"] if k.startswith("needle.")]:
            got = truth["answers"][k]["value"] + tail["answers"][k]["value"]
            truth["answers"]["mixed." + k] = {"rows": 1, "value": got}
    write_json(args.out, truth)


def cli_base(args):
    return [args.cli_bin, "--store", "s3", "--tenant-hash-unkeyed"]


def run_cli(cmd, stop_at=None):
    """A ravel-cli command's stdout lines. stop_at ends the read (and the
    child) at the first line starting with it: rspan inspect prints every
    record after the header."""
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors="replace")
    lines = []
    for line in proc.stdout:
        line = line.rstrip("\n")
        if stop_at and line.startswith(stop_at):
            proc.kill()
            proc.wait()
            return lines
        lines.append(line)
    err = proc.stderr.read()
    if proc.wait() != 0:
        die(f"{' '.join(cmd[4:])}: exit {proc.returncode}: {err.strip()[:300]}")
    return lines


def kv_fields(text):
    return dict(kv.split("=", 1) for kv in text.split() if "=" in kv)


def as_int(v):
    return int(v) if isinstance(v, str) and v.isdigit() else v


def parse_catalog_inspect(lines, catalog_objects):
    """catalog inspect's text: the HEAD header, each snapshot part with its
    entry count, and each key-index leaf the part carries with its size from
    the census."""
    doc = {"head": None, "parts": []}
    part = None
    for line in lines:
        s = line.strip()
        if "HEAD" in s and "absent" in s:
            doc["head"] = None
            doc["head_absent"] = s
            return doc
        if line.startswith("  part["):
            f = kv_fields(s)
            part = {"index": int(s[5:s.index("]")]), "range": f.get("range"), "key": f.get("key"),
                    "bytes": as_int(f.get("size")), "entry_count": as_int(f.get("entry_count")), "leaves": []}
            doc["parts"].append(part)
        elif s.startswith("field=") and part is not None:
            f = kv_fields(s)
            part["leaves"].append({"field": f.get("field"), "entry_count": as_int(f.get("entry_count")),
                                   "uncovered_count": as_int(f.get("uncovered_count")), "key": f.get("key"),
                                   "bytes": catalog_objects.get(f.get("key"))})
        elif s.startswith("key_index (field 8):") and part is not None:
            part["key_index"] = s.split(":", 1)[1].strip()
        elif part is None and ":" in s and not s.startswith("parts"):
            k, _, v = s.partition(":")
            if k in ("format_version", "watermark_hour", "folder_id", "created_unix_ns", "shard_count", "signal"):
                if doc["head"] is None:
                    doc["head"] = {}
                doc["head"][k] = as_int(v.strip())
        elif s.startswith("parts:"):
            doc["parts_declared"] = int(s.split(":", 1)[1])
    doc["part_count"] = len(doc["parts"])
    doc["parts_with_leaves"] = sum(1 for p in doc["parts"] if p["leaves"])
    counts = [p["entry_count"] for p in doc["parts"]]
    # A part whose entry count did not parse leaves the total unknown, not short.
    doc["entries"] = sum(counts) if all(isinstance(c, int) for c in counts) else None
    return doc


def parse_object_inspect(lines):
    """rspan/rlog inspect's header: version, the section table, KEY_IDX and
    its fields, the field directory, and the footer length (the bytes after
    the last section)."""
    doc = {"sections": [], "key_idx": None, "key_idx_fields": [], "field_dir": []}
    where = "header"
    for line in lines:
        s = line.strip()
        if not line.startswith(" "):
            if s.startswith("key_idx:"):
                doc["key_idx"] = {k: as_int(v) for k, v in kv_fields(s[8:]).items()}
                where = "key_idx"
            elif s == "sections:":
                where = "sections"
            elif s.startswith("field_dir"):
                where = "field_dir"
            elif where == "header" and ":" in s:
                k, _, v = s.partition(":")
                doc[k] = as_int(v.strip())
            else:
                where = "other"
            continue
        if where == "sections":
            doc["sections"].append({k: as_int(v) for k, v in kv_fields(s).items()})
        elif where == "key_idx" and s.startswith("field "):
            doc["key_idx_fields"].append({k: as_int(v) for k, v in kv_fields(s).items()
                                          if k in ("name", "key_type", "entries", "buckets", "empty_buckets")})
        elif where == "field_dir" and s.startswith("column_id="):
            doc["field_dir"].append({k: as_int(v) for k, v in kv_fields(s).items()})
    ends = [sec["offset"] + sec["len"] for sec in doc["sections"] if isinstance(sec.get("offset"), int)]
    total = doc.get("total_size")
    doc["footer_len"] = total - max(ends) if ends and isinstance(total, int) else None
    return doc


# The header fields rspan and rlog inspect print for an object's event range.
RANGE_FIELDS = {"spans": ("min_start_ts_ns", "max_end_ts_ns"), "logs": ("min_ts_ns", "max_ts_ns")}


def object_ranges(base, keys_path, tenant_hash, signal):
    """Every data object (is_data_key) of one tenant and signal in a census keys
    file, with its size and the event range its header gives; a field the
    header does not carry is null."""
    prefix = f"t/{tenant_hash}/{'l' if signal == 'logs' else 's'}/"
    lo, hi = RANGE_FIELDS[signal]
    out = []
    with open(keys_path) as f:
        keys = sorted((line.rstrip("\n").split("\t", 1) for line in f if "\t" in line), key=lambda p: p[1])
    for size, key in keys:
        if not (key.startswith(prefix) and is_data_key(key.split("/")[2:])):
            continue
        tool = "rlog" if signal == "logs" else "rspan"
        obj = parse_object_inspect(run_cli(base + [tool, "inspect", key], stop_at="records ("))
        out.append({"key": key, "bytes": as_int(size), "min_ns": obj.get(lo), "max_ns": obj.get(hi)})
    return out


def cmd_inspect_stamp(args):
    """The per-state inspect stamps of one tenant and signal: catalog inspect,
    rspan/rlog inspect of the census's largest data object, inspect kidx of
    the first leaf, and typed-attr-column show, each as
    stamps/STATE.LABEL.{catalog,object,kidx}.json and .tenant.txt."""
    with open(args.census) as f:
        census = json.load(f)
    tc = (census.get("tenants") or {}).get(args.tenant)
    if tc is None:
        die(f"inspect-stamp: census {args.census} has no tenant {args.tenant}")
    base = cli_base(args)
    stem = os.path.join(args.out_dir, f"{args.state}.{args.label}")
    meta = {"state": args.state, "tenant": args.tenant, "signal": args.signal, "label": args.label,
            "at": utc_now()}
    store_signal = "logs" if args.signal == "logs" else "spans"
    lines = run_cli(base + ["catalog", "inspect", "--tenant", args.tenant, "--signal", store_signal])
    catalog = parse_catalog_inspect(lines, tc.get("catalog_objects", {}))
    write_json(stem + ".catalog.json", {**meta, **catalog})
    data = (tc.get("data") or {}).get(store_signal)
    if data and data.get("largest"):
        tool = "rlog" if store_signal == "logs" else "rspan"
        obj = parse_object_inspect(run_cli(base + [tool, "inspect", data["largest"]], stop_at="records ("))
        write_json(stem + ".object.json", {**meta, "key": data["largest"], "bytes": data["largest_bytes"],
                                           "objects": data["objects"], **obj})
    else:
        write_json(stem + ".object.json", {**meta, "key": None, "reason": "no data object in the census"})
    if args.ranges_keys:
        write_json(stem + ".ranges.json", {**meta, "objects": object_ranges(base, args.ranges_keys, tc["hash"],
                                                                            store_signal)})
    leaf = next((lf for p in catalog["parts"] for lf in p["leaves"]), None)
    if leaf:
        kidx = {}
        for line in run_cli(base + ["inspect", "kidx", leaf["key"]]):
            k, sep, v = line.strip().partition(": ")
            if sep:
                kidx[k] = as_int(v)
        write_json(stem + ".kidx.json", {**meta, "key": leaf["key"], **kidx})
    else:
        write_json(stem + ".kidx.json", {**meta, "key": None, "reason": "no key-index leaf in this state"})
    show = run_cli(base + ["typed-attr-column", "show", args.tenant])
    with open(stem + ".tenant.txt", "w") as f:
        f.write("\n".join(show) + "\n")


# The logs columns whose present_blocks count a needle's storage blocks: tag
# holds every needle, each needle_<tier> column only its own tier's.
NEEDLE_COLUMNS = ("tag", "needle_rare", "needle_sparse", "needle_common")


def needle_totals(objs):
    """Block, record and per-column present_blocks/null_count totals over
    parsed rlog inspect headers. A total no object can vouch for is None,
    never 0: every total when an object printed no field directory or no
    integer block_count/record_count, a column's totals when an object listed
    it without an integer count or when no object listed it at all. A column
    absent from one object's non-empty directory holds no value there and
    adds 0."""
    tot = {"block_count": 0, "record_count": 0}
    for col in NEEDLE_COLUMNS:
        tot[f"{col}_present_blocks"] = 0
        tot[f"{col}_null_count"] = 0
    seen = set()
    for obj in objs:
        if not obj.get("field_dir") or not all(isinstance(obj.get(k), int) for k in ("block_count", "record_count")):
            return {k: None for k in tot}
        for k in ("block_count", "record_count"):
            tot[k] += obj[k]
        for col in NEEDLE_COLUMNS:
            entry = next((c for c in obj["field_dir"] if c.get("name") == col), None)
            if entry is None:
                continue
            seen.add(col)
            for field in ("present_blocks", "null_count"):
                k = f"{col}_{field}"
                v = entry.get(field)
                tot[k] = tot[k] + v if isinstance(v, int) and isinstance(tot[k], int) else None
    for col in NEEDLE_COLUMNS:
        if col not in seen:
            tot[f"{col}_present_blocks"] = tot[f"{col}_null_count"] = None
    return tot


def cmd_needle_blocks(args):
    """Storage blocks holding each needle, from rlog inspect's field
    directory: a needle column is null on every row but its needle's, so its
    present_blocks summed over the logs objects is the storage block count.
    gen_corpus predicts the count in units of its own row order (truth
    needle_blocks); storage sorts rows by (stream_ref, ts) within a shard and
    hour, so only needle_rare (one row, so one block whatever the order)
    compares exactly."""
    with open(args.census) as f:
        census = json.load(f)
    tc = census["tenants"][args.tenant]
    prefix = f"t/{tc['hash']}/l/"
    keys = []
    with open(args.keys) as f:
        for line in f:
            size, _, key = line.rstrip("\n").partition("\t")
            if key.startswith(prefix) and is_data_key(key.split("/")[2:]):
                keys.append(key)
    keys.sort()
    picked = keys if args.max_objects <= 0 else keys[: args.max_objects]
    objs = [parse_object_inspect(run_cli(cli_base(args) + ["rlog", "inspect", key], stop_at="records ("))
            for key in picked]
    with open(args.truth) as f:
        truth = json.load(f)
    write_json(args.out, {"tenant": args.tenant, "objects_total": len(keys), "objects_inspected": len(picked),
                          **needle_totals(objs), "truth_needle_blocks": truth["needle_blocks"],
                          "truth_needles": truth["needles"], "truth_block_rows": truth["generator"]["block_rows"]})


# --- plan ---------------------------------------------------------------------


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path, doc):
    text = json.dumps(doc, sort_keys=True, indent=1) + "\n"
    if path == "-":
        sys.stdout.write(text)
    else:
        with open(path, "w") as f:
            f.write(text)


def load_toml(path):
    with open(path, "rb") as f:
        return tomllib.load(f)


def placeholders(truth, run):
    saved = truth["saved_traces"]
    cell = truth["failing_cell"]
    p = {
        "run": str(run),
        "trace": saved[run % len(saved)]["trace_id"],
        "cell_service": cell["service"],
        "cell_start_ns": str(cell["hour_start_ns"]),
        "cell_end_ns": str(cell["hour_end_ns"]),
        "request_present": truth["request_id"]["present"],
        "request_absent": truth["request_id"]["absent"],
    }
    for size, ids in truth["in_lists"]["trace_id"].items():
        p[f"in_trace_ids_x_{size}"] = ", ".join(f"X'{t}'" for t in ids)
    for size, ids in truth["in_lists"]["request_id"].items():
        p[f"in_request_ids_{size}"] = ", ".join(f"'{r}'" for r in ids)
    # The 1,001-key controls: the largest list plus one key outside it.
    extra = truth.get("control_extra") or {}
    for field, fmt, name in (("trace_id", "X'{}'", "in_trace_ids_x"), ("request_id", "'{}'", "in_request_ids")):
        sizes = sorted(truth["in_lists"][field], key=int)
        if not sizes:
            continue
        ids = list(truth["in_lists"][field][sizes[-1]])
        if extra.get(field):
            ids.append(extra[field])
        p[f"{name}_{int(sizes[-1]) + 1}"] = ", ".join(fmt.format(i) for i in ids)
    for k, jc in truth["journey_c"].items():
        p[f"jc_start_ns_{k}"] = str(jc["start_ns"])
        p[f"jc_cut_ns_{k}"] = str(jc["cut_ns"])
    if truth.get("erasure_victim"):
        p["erasure_victim_rid"] = truth["erasure_victim"]["request_id"]
    return p


def fill(text, p):
    out = text
    for k, v in p.items():
        out = out.replace("{" + k + "}", v)
    if "{" in out and "}" in out:
        import re

        left = re.findall(r"\{([a-z_0-9]+)\}", out)
        if left:
            die(f"unfilled placeholders {sorted(set(left))} in {text[:80]!r}")
    return out


def window_bounds(st, truth, run, ingest_shift_ns):
    """(start_s, end_s) as Unix float seconds, or (None, None) for no range."""
    w = st.get("window", "none")
    if w == "none":
        return None, None
    gen = truth["generator"]
    if st.get("anchor", "trace") == "cell":
        cell = truth["failing_cell"]
        centre = (cell["hour_start_ns"] + cell["hour_end_ns"]) // 2
    else:
        saved = truth["saved_traces"]
        centre = saved[run % len(saved)]["start_ns"]
    if w == "1h":
        start, end = centre - HOUR_NS // 2, centre + HOUR_NS // 2
    elif w == "narrow":
        start, end = centre - NARROW_NS // 2, centre + NARROW_NS // 2
    elif w == "wide":
        start = gen["start_hour"] * HOUR_NS
        end = start + (gen["hours"] + 1) * HOUR_NS
    else:
        die(f"statement {st['id']}: unknown window {w!r}", 64)
    start += ingest_shift_ns
    end += ingest_shift_ns
    return start / 1e9, end / 1e9


def sub_truth(truth, name):
    """truth.json itself, or one of the corpora merge-truth folded into it
    (sub.<name>) with its answers under the <name>. prefix."""
    if not name:
        return truth, ""
    sub = (truth.get("sub") or {}).get(name)
    if sub is None:
        die(f"plan: truth.json has no sub.{name}", 64)
    return sub, name + "."


def cmd_plan(args):
    """Every call of one state, statement by statement: a statement's cold
    tries, then its warm repeats, so a warm call follows its own cold calls
    with no restart between. A state this build cannot reach
    (--unsupported-reason) plans one unsupported line per statement. --temp
    inflight plans every try as an in-flight call: no restart, no warm
    repeat (the compaction_in_flight state)."""
    doc = load_toml(args.statements)
    truth = json.load(open(args.truth)) if args.truth else None
    hits = load_corpus(args.hits_corpus) if args.hits_corpus else {}
    lines = []
    for st in doc["statement"]:
        if args.state not in st.get("states", []):
            continue
        tenant = st.get("tenant", "inv")
        if tenant == "hits" and not args.hits:
            continue
        if args.tenant:
            tenant = args.tenant
        base = {"statement": st["id"], "state": args.state, "tenant": tenant, "kind": st["kind"]}
        reason = args.unsupported_reason or st.get("unsupported_reason")
        if reason:
            lines.append({**base, "temp": "cold", "try": 0, "unsupported_reason": reason})
            continue
        if truth is None:
            die("plan: --truth is required for a state that runs", 64)
        if st.get("control"):
            for entry in hits.get("entries", []):
                lines.append({**base, "statement": f"control_{entry['id']}", "temp": "cold", "try": 0,
                              "path": "/api/v1/sql", "body": {"query": entry["sql"], "start": 0},
                              "truth_key": "hits.control"})
            continue
        t, prefix = sub_truth(truth, args.sub or st.get("truth_sub"))
        if st.get("requires") and not t.get(st["requires"]):
            lines.append({**base, "temp": "cold", "try": 0,
                          "not_run_reason": f"truth.json {prefix}{st['requires']} is null at this corpus size"})
            continue
        if args.temp == "inflight":
            temps = [("inflight", i) for i in range(args.runs)]
        else:
            temps = [("cold", i) for i in range(args.runs)]
            if not st.get("cold_only", True):
                temps += [("warm", i) for i in range(args.warm_runs)]
        for temp, i in temps:
            # A warm repeat reads the last cold call's key, so its data is
            # what that call just pulled in.
            run = (args.runs - 1 if temp == "warm" else i) if st.get("per_run") == "saved_trace" else 0
            p = placeholders(t, run)
            start, end = window_bounds(st, t, run, args.ingest_shift_ns)
            key = (st.get("truth_key_in") or {}).get(args.state, st["truth_key"])
            line = {**base, "temp": temp, "try": i, "truth_key": prefix + fill(key, p)}
            if st.get("oracle"):
                line["oracle"] = st["oracle"]
            if st.get("expect_refusal_on_main"):
                line["expect_refusal_on_main"] = True
            if st["kind"] == "trace_get":
                q = {}
                if start is not None:
                    q = {"start": f"{start:.3f}", "end": f"{end:.3f}"}
                line["path"] = f"/api/traces/{p['trace']}" + ("?" + urllib.parse.urlencode(q) if q else "")
            else:
                if st.get("corpus_id"):
                    sql = next((e["sql"] for e in hits.get("entries", []) if e["id"] == st["corpus_id"]), None)
                    if sql is None:
                        die(f"statement {st['id']}: corpus_id {st['corpus_id']} not in the hits corpus")
                else:
                    sql = fill(st["sql"], p)
                body = {"query": sql}
                if start is not None:
                    body["start"] = start
                    body["end"] = end
                else:
                    body["start"] = 0
                line["path"] = "/api/v1/sql"
                line["body"] = body
            lines.append(line)
    with open(args.out, "w") as f:
        for line in lines:
            f.write(json.dumps(line, sort_keys=True) + "\n")
    print(len(lines))


def load_corpus(path):
    with open(path) as f:
        return json.load(f)


# --- call ---------------------------------------------------------------------


def darwin_rusage(pid):
    """(user + system CPU seconds, lifetime peak physical footprint bytes) of
    pid from libproc's proc_pid_rusage (RUSAGE_INFO_V4): the local lane's
    stand-in for /proc on macOS, where no process exposes VmHWM. Its CPU
    times are mach absolute time units, scaled by mach_timebase_info."""
    import ctypes
    import ctypes.util

    class Timebase(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]

    system = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
    libproc = ctypes.CDLL(ctypes.util.find_library("proc") or "/usr/lib/libSystem.B.dylib")
    tb = Timebase()
    system.mach_timebase_info(ctypes.byref(tb))
    # rusage_info_v4: a 16-byte uuid, then 35 uint64 fields; 40 is headroom.
    buf = (ctypes.c_uint64 * 40)()
    if libproc.proc_pid_rusage(int(pid), 4, ctypes.byref(buf)) != 0:
        raise OSError(f"proc_pid_rusage({pid}) failed")
    f = list(buf)[2:]
    cpu_s = (f[0] + f[1]) * tb.numer / tb.denom / 1e9
    return cpu_s, f[28]


def proc_cpu_s(pid):
    """utime + stime of pid in seconds, from /proc/<pid>/stat (on macOS,
    darwin_rusage); None once the process is gone, on either platform (a
    server that died mid-call leaves nothing for the "after" reading, and the
    call must still exit 75 for run.sh to record the death)."""
    try:
        if sys.platform == "darwin":
            return darwin_rusage(pid)[0]
        with open(f"/proc/{pid}/stat") as f:
            stat = f.read()
    except OSError:
        return None
    fields = stat[stat.rindex(")") + 2 :].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return (int(fields[11]) + int(fields[12])) / ticks


def proc_hwm_bytes(pid):
    """VmHWM of pid (on macOS, the lifetime peak physical footprint); None
    once the process is gone, on either platform."""
    try:
        if sys.platform == "darwin":
            return darwin_rusage(pid)[1]
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None
    return None


def answer_values(columns, rows):
    """The first column's values, sorted, as strings; a digest past ROW_SET_MAX."""
    vals = sorted(str(r[0]) if r and r[0] is not None else "" for r in rows)
    if len(vals) <= ROW_SET_MAX:
        return {"values": vals}
    return {"values_sha256": hashlib.sha256("\n".join(vals).encode()).hexdigest()}


def complexity_refusal(code, raw):
    """The server's own text when it refused a statement for its size (ravel-sql
    complexity_guard: HTTP 400 naming the statement complexity), else None."""
    if code != 400:
        return None
    text = (raw or b"").decode("utf-8", "replace")
    try:
        doc = json.loads(text)
        if isinstance(doc, dict) and isinstance(doc.get("error"), str):
            text = doc["error"]
    except ValueError:
        pass
    return text.strip()[:500] if "complexity" in text else None


def unsupported_line(statement, state, temp, reason):
    return {
        "statement": statement,
        "state": state,
        "temp": temp,
        "try": 0,
        "outcome": "unsupported-on-this-build",
        "reason": reason,
    }


def append_line(path, line):
    with open(path, "a") as f:
        f.write(json.dumps(line, sort_keys=True) + "\n")


def cmd_call(args):
    plan = json.loads(args.line)
    if plan.get("unsupported_reason"):
        append_line(args.calls, unsupported_line(plan["statement"], plan["state"], plan["temp"],
                                                 plan["unsupported_reason"]))
        return
    if plan.get("not_run_reason"):
        append_line(args.calls, {"statement": plan["statement"], "state": plan["state"], "temp": plan["temp"],
                                 "try": 0, "outcome": "not-run", "reason": plan["not_run_reason"]})
        return
    if args.not_run:
        append_line(args.calls, {"statement": plan["statement"], "state": plan["state"], "temp": plan["temp"],
                                 "try": plan["try"], "outcome": "not-run", "reason": args.not_run})
        return
    if args.deadline_unix_s and time.time() >= args.deadline_unix_s:
        append_line(args.calls, {"statement": plan["statement"], "state": plan["state"], "temp": plan["temp"],
                                 "try": plan["try"], "outcome": "not-run",
                                 "reason": "run deadline (max_minutes) passed before this call"})
        return
    auth = open(os.path.join(args.auth_dir, plan["tenant"] + ".auth")).read().strip()
    name, _, value = auth.partition(":")
    headers = {name.strip(): value.strip(), "Accept": "application/json"}
    data = None
    method = "GET"
    if plan["kind"] == "sql":
        method = "POST"
        data = json.dumps(plan["body"]).encode()
        headers["Content-Type"] = "application/json"
    cpu0 = proc_cpu_s(args.pid)
    started = time.time()
    t0 = time.monotonic()
    req = urllib.request.Request(args.server.rstrip("/") + plan["path"], data=data, method=method, headers=headers)
    code = None
    try:
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:
            code, raw = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, raw = e.code, e.read()
    except (OSError, http.client.HTTPException) as e:
        # A refused or reset connection, a timeout, or a body cut short
        # (IncompleteRead, an HTTPException rather than an OSError): no answer.
        # run.sh decides from the server process whether that was a death.
        code, raw = None, f"{type(e).__name__}: {e}".encode()
    wall = time.monotonic() - t0
    cpu1 = proc_cpu_s(args.pid)
    line = {
        "statement": plan["statement"],
        "state": plan["state"],
        "temp": plan["temp"],
        "try": plan["try"],
        "tenant": plan["tenant"],
        "truth_key": plan.get("truth_key"),
        "http_status": code,
        "started_unix_s": round(started, 3),
        "wall_s": round(wall, 6),
        "server_cpu_s": round(cpu1 - cpu0, 6) if cpu0 is not None and cpu1 is not None else None,
        "server_hwm_bytes": proc_hwm_bytes(args.pid),
    }
    if plan.get("oracle"):
        line["oracle"] = plan["oracle"]
    try:
        doc = json.loads(raw)
    except ValueError:
        doc = None
    refusal = complexity_refusal(code, raw)
    if refusal:
        # Only a statement planned as a cap probe may read as unsupported; any
        # other refusal is a defect evaluate.py reports.
        line["outcome"] = "unsupported-on-this-build" if plan.get("expect_refusal_on_main") else "refused"
        line["reason"] = f"server refused the statement (HTTP {code}): {refusal}"
        append_line(args.calls, line)
        return
    if code == 200 and isinstance(doc, dict) and isinstance(doc.get("data"), dict):
        rows = doc["data"].get("rows", [])
        cols = [c.get("name") for c in doc["data"].get("columns", [])]
        line["outcome"] = "ok"
        line["rows"] = len(rows)
        line["columns"] = cols
        if plan["kind"] == "trace_get":
            idx = cols.index("span_id") if "span_id" in cols else 0
            line.update(answer_values(cols, [[r[idx]] for r in rows]))
        else:
            line.update(answer_values(cols, rows))
        if args.rq_stats:
            line["stats"] = doc.get("stats")
    else:
        line["outcome"] = "error"
        line["error"] = (raw or b"")[:500].decode("utf-8", "replace")
    if code is None:
        # No calls.jsonl line here: the caller (run.sh statements()) checks
        # whether the server process is still alive, and either records this
        # try itself (a dead server, richer diagnostic: signal, restart, kept
        # log) or treats it as an ordinary unreachable-server failure. Either
        # way, writing a line here would duplicate whichever one it writes.
        print(f"{plan['statement']} ({plan['state']}, {plan['temp']} try {plan['try']}): "
              f"connection failed: {line['error'][:200]}", file=sys.stderr)
        sys.exit(75)
    append_line(args.calls, line)
    if line["outcome"] != "ok":
        # The error is on record for evaluate.py; only a server that did not
        # answer at all stops the run.
        print(f"{plan['statement']} ({plan['state']}, {plan['temp']} try {plan['try']}): "
              f"HTTP {code}: {line['error'][:200]}", file=sys.stderr)


def cmd_unsupported(args):
    append_line(args.calls, unsupported_line(args.statement, args.state, args.temp, args.reason))


# --- measure ------------------------------------------------------------------


def cmd_measure(args):
    cmd = args.cmd[1:] if args.cmd and args.cmd[0] == "--" else args.cmd
    if not cmd:
        die("measure: no command", 64)
    started = time.time()
    t0 = time.monotonic()
    with open(args.stdout, "w") as out:
        proc = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT)
    wall = time.monotonic() - t0
    ru = resource.getrusage(resource.RUSAGE_CHILDREN)
    doc = {
        "argv0": os.path.basename(cmd[0]),
        "exit": proc.returncode,
        "started_unix_s": round(started, 3),
        "ended_unix_s": round(started + wall, 3),
        "wall_s": round(wall, 3),
        "cpu_s": round(ru.ru_utime + ru.ru_stime, 3),
        "peak_rss_bytes": ru.ru_maxrss * 1024,
    }
    write_json(args.out, doc)
    sys.exit(proc.returncode)


# --- stamps and config ---------------------------------------------------------


def extract_report(text):
    """The JSON report a ravel-cli run printed: the whole output when it is
    one JSON document (`--json`), else the last JSON object that starts a
    line (a pretty-printed `--json` document beside stderr lines, which
    `measure` writes to the same file), else the last line carrying a JSON
    object (load's `load report json : {...}`)."""
    try:
        doc = json.loads(text)
        if isinstance(doc, dict):
            return doc
    except ValueError:
        pass
    decoder = json.JSONDecoder()
    found = None
    offset = 0
    for line in text.splitlines(keepends=True):
        if line.startswith("{"):
            try:
                doc, end = decoder.raw_decode(text, offset)
            except ValueError:
                doc, end = None, offset
            rest = text[end:].split("\n", 1)[0]
            if isinstance(doc, dict) and not rest.strip():
                found = doc
        offset += len(line)
    if found is not None:
        return found
    for line in reversed(text.splitlines()):
        i = line.find("{")
        if i < 0:
            continue
        try:
            doc = json.loads(line[i:])
        except ValueError:
            continue
        if isinstance(doc, dict):
            return doc
    return None


def cmd_stamp(args):
    """One stamp document from FIELD arguments: k=string, k:=JSON, k@=JSON
    file, k%=tool output whose report is extracted (null when it has none)."""
    doc = {}
    for field in args.fields:
        for op in (":=", "@=", "%=", "="):
            k, sep, v = field.partition(op)
            if sep and k and all(c.isalnum() or c == "_" for c in k):
                break
        else:
            die(f"stamp: cannot read field {field!r}", 64)
        if op == "=":
            doc[k] = v
        elif op == ":=":
            try:
                doc[k] = json.loads(v)
            except ValueError:
                die(f"stamp: {k}: not JSON: {v!r}", 64)
        elif op == "@=":
            try:
                with open(v) as f:
                    doc[k] = json.load(f)
            except (OSError, ValueError) as e:
                die(f"stamp: {k}: {e}")
        else:
            try:
                with open(v) as f:
                    doc[k] = extract_report(f.read())
            except OSError as e:
                die(f"stamp: {k}: {e}")
    if args.append:
        append_line(args.file, doc)
    else:
        write_json(args.file, doc)


def cmd_get(args):
    """Print one run.toml value for bash: a list space-separated, a bool as
    true/false. Absent and no --default exits 1."""
    v = load_toml(args.toml)
    for part in args.key.split("."):
        if not isinstance(v, dict) or part not in v:
            if args.default is not None:
                print(args.default)
                return
            die(f"{args.toml}: no {args.key}")
        v = v[part]
    if isinstance(v, bool):
        print("true" if v else "false")
    elif isinstance(v, list):
        print(" ".join(str(x) for x in v))
    elif isinstance(v, dict):
        die(f"{args.toml}: {args.key} is a table", 64)
    else:
        print(v)


def cmd_server_extra_flags(args):
    """run.toml [server] extra_flags, one element per line for run.sh to read
    into an array (no shell evaluation, no word splitting, no globbing): each
    element is one argv token, so a value goes in the same element
    ("--flag=value"). Refuses (exit 64, naming the element) any entry that
    contains whitespace or does not start with "--"."""
    v = load_toml(args.toml).get("server", {}).get("extra_flags", [])
    if not isinstance(v, list):
        die("run.toml server.extra_flags is not an array", 64)
    for elem in v:
        if not isinstance(elem, str) or any(c.isspace() for c in elem) or not elem.startswith("--"):
            die(f"run.toml server.extra_flags: {elem!r} is not a single --flag token", 64)
    for elem in v:
        print(elem)


def strip_ansi(text):
    import re

    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def cmd_warmup(args):
    """Wait for the server's ADR-0046 startup cache warmup to finish and print
    what it warmed, so a cold call never overlaps it. A log that never says
    so within the timeout prints a null with the reason."""
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            with open(args.log, errors="replace") as f:
                text = strip_ansi(f.read())
        except OSError as e:
            die(f"warmup: {e}")
        for line in text.splitlines():
            if "cache warmup: pass complete" in line:
                fields = dict(kv.split("=", 1) for kv in line.split() if "=" in kv)
                out = {"parts_warmed": int(fields["parts_warmed"]) if fields.get("parts_warmed", "").isdigit() else None}
                if fields.get("elapsed_ms", "").isdigit():
                    out["elapsed_ms"] = int(fields["elapsed_ms"])
                print(json.dumps(out, sort_keys=True))
                return
        if time.monotonic() > deadline:
            print(json.dumps({"parts_warmed": None,
                              "reason": f"no 'cache warmup: pass complete' line within {args.timeout:g}s"}))
            return
        time.sleep(0.2)


def stride_refusal(ends_ns, batch_rows, start_hour, stride, lag_ns, batches):
    """The reason a strided load of rows with these event times (in file
    order) would refuse a batch, or None. Mirrors `HourStrideLane::begin_batch`
    (services/ravel-cli/src/load/hour_stride.rs:293): batch i, the file's
    rows [i * batch_rows, (i + 1) * batch_rows), goes into ingest hour
    start_hour + i * stride and is refused (EventTimePastLag, line 311) when
    its latest event time is not before that hour's end plus the lag. Also
    refuses a file that deals other than `batches` batches, since each batch
    is one of the state's hours."""
    dealt = -(-len(ends_ns) // batch_rows)
    if dealt != batches:
        return (f"the corpus deals {dealt} batches of {batch_rows} rows ({len(ends_ns)} rows), not the "
                f"{batches} strided ingest hours the state loads")
    for i in range(dealt):
        top = max(ends_ns[i * batch_rows:(i + 1) * batch_rows])
        hour = start_hour + i * stride
        bound = (hour + 1) * HOUR_NS + lag_ns
        if top >= bound:
            return (f"--ingest-hour-stride would refuse batch {i} before submitting it: its latest event time "
                    f"{top} ns is not before {bound} ns, the end of its ingest hour {hour} plus max_ingest_lag "
                    f"({lag_ns} ns) (EventTimePastLag, services/ravel-cli/src/load/hour_stride.rs:311)")
    return None


def cmd_stride_check(args):
    """Exit 0 printing the batch count when every batch passes; exit 1 with
    the refusal on stdout when the loader would refuse a batch (run.sh
    records it as the state's not-run reason); exit 70 with the error on
    stderr when the check itself could not run (no pyarrow, an unreadable
    file, a null event time), which run.sh records as a failed state, and
    64 on a bad argument. Exit 1 means refused and nothing else."""
    try:
        import pyarrow.parquet as pq
    except ImportError as e:
        die(f"stride-check: {e}", 70)
    if args.batch_rows < 1:
        die("stride-check: --batch-rows must be at least 1", 64)
    try:
        col = pq.read_table(args.parquet, columns=[args.event_column]).column(0)
        ends = [int(v) for v in col.cast("int64").to_pylist()]
    except Exception as e:  # pyarrow raises its own error types; None fails int()
        die(f"stride-check: {args.parquet}: {e}", 70)
    why = stride_refusal(ends, args.batch_rows, args.ingest_hour_start, args.stride, args.lag_ns, args.batches)
    if why:
        print(why)
        sys.exit(1)
    print(json.dumps({"batches": args.batches, "batch_rows": args.batch_rows, "rows": len(ends)}))


def cmd_kv_report(args):
    """A text tool report as JSON: each `key: value` line (an integer value
    as a number), a count per `outcome=X` line (maintain compact-tenant), and
    the first 50 lines verbatim."""
    try:
        with open(args.log) as f:
            lines = f.read().splitlines()
    except OSError as e:
        die(f"kv-report: {e}")
    doc = {"fields": {}, "outcomes": {}, "lines": lines[:50]}
    for line in lines:
        k, sep, v = line.partition(":")
        k, v = k.strip(), v.strip()
        if sep and k and all(c.isalnum() or c in "_ " for c in k):
            doc["fields"][k.replace(" ", "_")] = int(v) if v.isdigit() else v
        for kv in line.split():
            if kv.startswith("outcome="):
                o = kv[len("outcome="):]
                doc["outcomes"][o] = doc["outcomes"].get(o, 0) + 1
    print(json.dumps(doc, sort_keys=True))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd_name", required=True)

    st = sub.add_parser("stamp")
    st.add_argument("--file", required=True)
    st.add_argument("--append", action="store_true")
    st.add_argument("fields", nargs="*")
    st.set_defaults(fn=cmd_stamp)

    g = sub.add_parser("get")
    g.add_argument("toml")
    g.add_argument("key")
    g.add_argument("--default")
    g.set_defaults(fn=cmd_get)

    sef = sub.add_parser("server-extra-flags")
    sef.add_argument("toml")
    sef.set_defaults(fn=cmd_server_extra_flags)

    w = sub.add_parser("warmup")
    w.add_argument("--log", required=True)
    w.add_argument("--timeout", type=float, default=300)
    w.set_defaults(fn=cmd_warmup)

    cl = sub.add_parser("census-listing")
    cl.add_argument("--listing", required=True)
    cl.add_argument("--bucket", required=True)
    cl.add_argument("--out", default="-")
    cl.add_argument("--tenants", help="JSON {tenant: hash}: census each tenant's objects too")
    cl.add_argument("--keys-out", help="write every key and its size, one per line")
    cl.set_defaults(fn=cmd_census_listing)

    s = sub.add_parser("s3")
    s.add_argument("op", choices=["wait", "mkbucket", "census"])
    s.add_argument("--timeout", type=float, default=60)
    s.add_argument("--prefix", default="")
    s.add_argument("--out", default="-")
    s.add_argument("--tenants", help="JSON {tenant: hash}: census each tenant's objects too")
    s.add_argument("--keys-out", help="write every key and its size, one per line")
    s.set_defaults(fn=cmd_s3)

    mt = sub.add_parser("merge-truth")
    mt.add_argument("--base", required=True)
    mt.add_argument("--sub", action="append", default=[], help="NAME=PATH, repeatable")
    mt.add_argument("--out", required=True)
    mt.set_defaults(fn=cmd_merge_truth)

    ins = sub.add_parser("inspect-stamp")
    ins.add_argument("--cli-bin", required=True)
    ins.add_argument("--census", required=True)
    ins.add_argument("--tenant", required=True)
    ins.add_argument("--signal", required=True, choices=["spans", "logs"])
    ins.add_argument("--label", required=True)
    ins.add_argument("--state", required=True)
    ins.add_argument("--out-dir", required=True)
    ins.add_argument("--ranges-keys", help="census keys file: also write STATE.LABEL.ranges.json")
    ins.set_defaults(fn=cmd_inspect_stamp)

    nb = sub.add_parser("needle-blocks")
    nb.add_argument("--cli-bin", required=True)
    nb.add_argument("--census", required=True)
    nb.add_argument("--keys", required=True)
    nb.add_argument("--tenant", required=True)
    nb.add_argument("--truth", required=True)
    nb.add_argument("--max-objects", type=int, default=0, help="0: every logs object")
    nb.add_argument("--out", required=True)
    nb.set_defaults(fn=cmd_needle_blocks)

    p = sub.add_parser("plan")
    p.add_argument("--statements", required=True)
    p.add_argument("--truth")
    p.add_argument("--state", required=True)
    p.add_argument("--runs", type=int, required=True)
    p.add_argument("--warm-runs", type=int, default=1)
    p.add_argument("--unsupported-reason")
    p.add_argument("--hits", action="store_true")
    p.add_argument("--hits-corpus")
    p.add_argument("--ingest-shift-ns", type=int, default=0)
    p.add_argument("--sub", help="read placeholders and truth keys from truth.json sub.<SUB>")
    p.add_argument("--tenant", help="run every statement against this tenant (many_parts_N)")
    p.add_argument("--temp", choices=["cold", "inflight"], default="cold")
    p.add_argument("--out", required=True)
    p.set_defaults(fn=cmd_plan)

    c = sub.add_parser("call")
    c.add_argument("--line", required=True)
    c.add_argument("--server", required=True)
    c.add_argument("--auth-dir", required=True, help="holds <tenant>.auth, one header line each")
    c.add_argument("--pid", type=int, required=True)
    c.add_argument("--calls", required=True)
    c.add_argument("--timeout", type=float, default=600)
    c.add_argument("--rq-stats", action="store_true")
    c.add_argument("--deadline-unix-s", type=float, default=0,
                   help="past this Unix time the call is recorded not-run instead of sent")
    c.add_argument("--not-run", default="",
                   help="record this planned call not-run with this reason, unsent (run.sh: after a server death)")
    c.set_defaults(fn=cmd_call)

    u = sub.add_parser("unsupported")
    u.add_argument("--statement", required=True)
    u.add_argument("--state", required=True)
    u.add_argument("--temp", default="cold")
    u.add_argument("--reason", required=True)
    u.add_argument("--calls", required=True)
    u.set_defaults(fn=cmd_unsupported)

    m = sub.add_parser("measure")
    m.add_argument("--out", required=True)
    m.add_argument("--stdout", required=True)
    m.add_argument("cmd", nargs=argparse.REMAINDER)
    m.set_defaults(fn=cmd_measure)

    sc = sub.add_parser("stride-check")
    sc.add_argument("--parquet", required=True)
    sc.add_argument("--event-column", default="end_ts", help="the column the loader reads a batch's latest event from")
    sc.add_argument("--batch-rows", type=int, required=True)
    sc.add_argument("--batches", type=int, required=True, help="the strided ingest hours the state loads")
    sc.add_argument("--ingest-hour-start", type=int, required=True)
    sc.add_argument("--stride", type=int, default=1)
    sc.add_argument("--lag-ns", type=int, default=MAX_INGEST_LAG_NS)
    sc.set_defaults(fn=cmd_stride_check)

    kv = sub.add_parser("kv-report")
    kv.add_argument("--log", required=True)
    kv.set_defaults(fn=cmd_kv_report)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
