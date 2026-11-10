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
  day-parts-window          a day_parts_N state's N whole UTC days, ending
                            before the current hour
  midnight-truth            add the midnight lookup (one request_id within
                            30 minutes of the window's first midnight) to a
                            day-parts corpus's truth.json (pyarrow)
  retention-cutoff          the retention window that expires one strided
                            batch's ingest bucket and not the next (pyarrow)
  tombstone-check           whether every bucket of an hour range that holds
                            a commit record holds its retire.tmb
  maint-records             compaction and rewrite records a maintain pass
                            added to one tenant and signal
  fold-binary               the retired_hour fold binary's SHA and whether it
                            carries run.toml's c1_sha and r2_sha
  day-parts-stamp           entries and slices per day part from a state's
                            catalog stamp

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
    add_over_cap(truth)
    write_json(args.out, truth)


# ADR-3692 decision 6's k: a key set holds at most this many keys
# (KEY_SET_MAX_KEYS, crates/ravel-query/src/key_index_route.rs). Row 11's
# over-cap control is one key over it.
KEY_SET_MAX_KEYS = 1024
OVER_CAP_KEYS = KEY_SET_MAX_KEYS + 1


def over_cap_padding(field, n, ids):
    """n ids of `field` the corpus does not hold, each with a key8 (the 8 bytes
    a KEY_IDX probe sorts and dedups on, crates/ravel-codec/src/key_idx.rs)
    no other padding id shares. gen_corpus.py writes request ids as
    req-<16 hex>-<row> or req-planted-*, so req-over-cap-absent-* is never
    one, and their key8 is a blake3 prefix. A trace id is an Id16 whose key8
    is its high 8 bytes, so padding j holds j there and zeros below; a random
    128-bit draw is that id only by hitting it exactly, and j is skipped when
    a listed id in `ids` already holds the same high 8 bytes."""
    if field == "trace_id":
        taken = {i[:16] for i in ids}
        highs = (f"{j:016x}" for j in range(1, n + len(taken) + 1))
        return [h + "0" * 16 for h in highs if h not in taken][:n]
    return [p for p in (f"req-over-cap-absent-{j:04d}" for j in range(1, n + len(ids) + 1)) if p not in ids][:n]


def add_over_cap(truth):
    """Row 11's over-cap control lists, truth["over_cap"][field]: the
    generator's largest IN list, its control_extra key (one the list does not
    hold, when the corpus has one), then padding the corpus does not hold, to
    OVER_CAP_KEYS keys. The padding adds no row, so each list's answer is the
    generator's answer for the largest list plus control_extra, under the
    OVER_CAP_KEYS truth key with its own requested count."""
    lists = truth.get("in_lists") or {}
    extra = truth.get("control_extra") or {}
    answers = truth.setdefault("answers", {})
    out = {}
    for field, keys in (("trace_id", ("in_trace_id.{}.spans", "in_trace_id.{}.logs")),
                        ("request_id", ("in_request_id.{}",))):
        sizes = sorted(lists.get(field) or {}, key=int)
        if not sizes:
            continue
        top = int(sizes[-1])
        ids = list(lists[field][sizes[-1]])
        if extra.get(field):
            ids.append(extra[field])
        ids += over_cap_padding(field, OVER_CAP_KEYS - len(ids), ids)
        out[field] = ids
        for key in keys:
            src = answers.get(key.format(top + 1))
            if src is None:
                continue
            per_key = src["requested"] // (top + 1) if "requested" in src else 1
            want = dict(src, requested=OVER_CAP_KEYS * per_key)
            want["capped"] = want["rows"] < want["requested"]
            answers[key.format(OVER_CAP_KEYS)] = want
    if out:
        truth["over_cap"] = out


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
    # Row 11's over-cap controls, as merge-truth built them.
    for field, fmt, name in (("trace_id", "X'{}'", "in_trace_ids_x"), ("request_id", "'{}'", "in_request_ids")):
        ids = (truth.get("over_cap") or {}).get(field)
        if ids:
            p[f"{name}_{len(ids)}"] = ", ".join(fmt.format(i) for i in ids)
    for k, jc in truth["journey_c"].items():
        p[f"jc_start_ns_{k}"] = str(jc["start_ns"])
        p[f"jc_cut_ns_{k}"] = str(jc["cut_ns"])
    if truth.get("erasure_victim"):
        p["erasure_victim_rid"] = truth["erasure_victim"]["request_id"]
    if truth.get("midnight"):
        p["midnight_request_ids_1"] = f"'{truth['midnight']['request_id']}'"
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
    if w == "midnight":
        mid = truth["midnight"]
        return (mid["window_start_ns"] + ingest_shift_ns) / 1e9, (mid["window_end_ns"] + ingest_shift_ns) / 1e9
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
    repeat (the compaction_in_flight state). --temp prefold plans every try
    as a prefold call: a cold call (restart first) sent before the state's
    fold, with no warm repeat (the day_parts_N states' unfolded reference)."""
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
        elif args.temp == "prefold":
            temps = [("prefold", i) for i in range(args.runs)]
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
    maxima = [max(ends_ns[i * batch_rows:(i + 1) * batch_rows]) for i in range(-(-len(ends_ns) // batch_rows))]
    return stride_refusal_maxima(maxima, len(ends_ns), batch_rows, start_hour, stride, lag_ns, batches)


def stride_refusal_maxima(maxima, rows, batch_rows, start_hour, stride, lag_ns, batches):
    """stride_refusal over each batch's latest event time, so a file too large
    to hold as a list is checked from its streamed per-batch maxima."""
    dealt = len(maxima)
    if dealt != batches:
        return (f"the corpus deals {dealt} batches of {batch_rows} rows ({rows} rows), not the "
                f"{batches} strided ingest hours the state loads")
    for i, top in enumerate(maxima):
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
        import pyarrow.compute  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except ImportError as e:
        die(f"stride-check: {e}", 70)
    if args.batch_rows < 1:
        die("stride-check: --batch-rows must be at least 1", 64)
    try:
        maxima, rows = batch_maxima(args.parquet, args.event_column, args.batch_rows)
    except Exception as e:  # pyarrow raises its own error types
        die(f"stride-check: {args.parquet}: {e}", 70)
    why = stride_refusal_maxima(maxima, rows, args.batch_rows, args.ingest_hour_start, args.stride, args.lag_ns,
                                args.batches)
    if why:
        print(why)
        sys.exit(1)
    print(json.dumps({"batches": args.batches, "batch_rows": args.batch_rows, "rows": rows}))


def batch_maxima(parquet, column, batch_rows):
    """([latest event time of each batch_rows-row batch, in file order], rows),
    streamed record batch by record batch: a multi-day corpus has more rows
    than a Python list holds comfortably. A null event time raises."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    maxima, rows = [], 0
    for rb in pq.ParquetFile(parquet).iter_batches(columns=[column], batch_size=1 << 16):
        arr = rb.column(0).cast("int64")
        if arr.null_count:
            raise ValueError(f"column {column} holds a null event time")
        off = 0
        while off < len(arr):
            i = rows // batch_rows
            take = min(len(arr) - off, (i + 1) * batch_rows - rows)
            top = pc.max(arr.slice(off, take)).as_py()
            if i == len(maxima):
                maxima.append(top)
            else:
                maxima[i] = max(maxima[i], top)
            off += take
            rows += take
    return maxima, rows


# --- ADR-3939's multi-day states (task H1b, #4048) ---------------------------


def day_parts_window(days, now_hour, end_day=None):
    """The hour-stride window of a day_parts_N state: N whole UTC days, the
    last ending before the current hour. Hours are unix hours, the index
    part ranges and ingest_hour_bucket count in. The loader refuses a
    window whose start plus its batches reaches the current ingest hour
    (services/ravel-cli/src/load/hour_stride.rs, "is not below the current
    ingest hour"), so this refuses the same window first. end_day (the last
    day, in unix days) defaults to the latest whole day that passes."""
    if days < 1:
        raise ValueError(f"--days {days} is not a positive day count")
    if end_day is None:
        end_day = (now_hour - 1) // 24 - 1
    start = (end_day - days + 1) * 24
    end = start + 24 * days
    if end >= now_hour:
        raise ValueError(f"{days} days ending with day {end_day} run to hour {end}, which is not below the "
                         f"current ingest hour {now_hour}: the strided load would refuse it")
    return {"days": days, "start_hour": start, "hours": 24 * days, "end_hour": end, "last_day": end_day,
            "now_hour": now_hour}


def cmd_day_parts_window(args):
    """Print a day_parts_N state's window as JSON; exit 1 with the reason when
    the window is not strictly before the current hour."""
    now = args.now_hour if args.now_hour is not None else int(time.time()) // 3600
    try:
        print(json.dumps(day_parts_window(args.days, now, args.end_day), sort_keys=True))
    except ValueError as e:
        die(f"day-parts-window: {e}")


MIDNIGHT_HALF_NS = HOUR_NS // 2


def cmd_midnight_truth(args):
    """Add the midnight lookup to a day-parts corpus's truth.json: the
    request_id of the logs row nearest the first UTC midnight of the window
    (start_hour + 24) within 30 minutes of it, that is no planted id (the
    present and absent ids, every in_lists request_id), and that occurs
    exactly once. Writes truth.midnight and the answer midnight.request_id.1
    in place; with no such row, midnight is null and the plan records the
    statement not-run through requires = "midnight"."""
    try:
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError as e:
        die(f"midnight-truth: {e}", 70)
    with open(args.truth) as f:
        truth = json.load(f)
    gen = truth["generator"]
    hour = gen["start_hour"] + 24
    m = hour * HOUR_NS
    planted = {truth["request_id"]["present"], truth["request_id"]["absent"]}
    for ids in truth["in_lists"]["request_id"].values():
        planted.update(ids)
    path = os.path.join(args.corpus, "logs.parquet")
    best = None
    try:
        for rb in pq.ParquetFile(path).iter_batches(columns=["ts", "request_id"], batch_size=1 << 16):
            ts = rb.column(0).cast("int64")
            near = pc.and_(pc.greater_equal(ts, m - MIDNIGHT_HALF_NS), pc.less(ts, m + MIDNIGHT_HALF_NS))
            if not pc.any(near).as_py():
                continue
            for t, rid in zip(ts.filter(near).to_pylist(), rb.column(1).filter(near).to_pylist()):
                if rid is None or rid in planted:
                    continue
                key = (abs(t - m), rid)
                if best is None or key < best[0]:
                    best = (key, t, rid)
        count = 0
        if best is not None:
            for rb in pq.ParquetFile(path).iter_batches(columns=["request_id"], batch_size=1 << 16):
                count += pc.sum(pc.equal(rb.column(0), best[2])).as_py() or 0
    except Exception as e:  # pyarrow raises its own error types
        die(f"midnight-truth: {path}: {e}", 70)
    if best is None:
        truth["midnight"] = None
    else:
        if count != 1:
            die(f"midnight-truth: request_id {best[2]} occurs {count} times, not once", 70)
        truth["midnight"] = {"hour": hour, "request_id": best[2], "ts_ns": best[1],
                             "window_start_ns": m - MIDNIGHT_HALF_NS, "window_end_ns": m + MIDNIGHT_HALF_NS}
        truth["answers"]["midnight.request_id.1"] = {"rows": 1, "row_set": [best[2]]}
    write_json(args.truth, truth)
    print(json.dumps(truth["midnight"], sort_keys=True))


def max_event_of_batch(parquet, column, batch_rows, k):
    maxima, _ = batch_maxima(parquet, column, batch_rows)
    if k + 1 >= len(maxima):
        raise ValueError(f"the corpus deals {len(maxima)} batches; batch {k + 1} does not exist")
    return maxima[k], maxima[k + 1]


def retention_window(max_k_ns, max_next_ns, now_s):
    """(cutoff_s, R_s) for a retention window that expires the bucket holding
    batch k and not the one holding batch k+1. ravel-maintain tombstones a
    bucket once its latest event time is before now - R (is_expired,
    crates/ravel-maintain/src/retention.rs), so the cutoff is the first
    whole second past batch k's latest event. Refused when batch k+1's latest
    event is before that cutoff: the same window would expire it too."""
    cutoff_s = -(-(max_k_ns + 1) // 1_000_000_000)
    if max_next_ns < cutoff_s * 1_000_000_000:
        raise ValueError(f"the next batch's latest event {max_next_ns} ns is before the cutoff {cutoff_s} s, so "
                         "a window expiring this batch would expire the next one too")
    if cutoff_s >= now_s:
        raise ValueError(f"the cutoff {cutoff_s} s is not in the past (now {now_s} s)")
    return cutoff_s, now_s - cutoff_s


def cmd_retention_cutoff(args):
    """Print {cutoff_s, retention_s, max_event_ns, next_max_event_ns} for the
    retention window that tombstones ingest bucket K of a strided load (the
    file's batch K) and no later one; exit 1 with the reason when none
    exists, 70 when the file could not be read."""
    try:
        top, nxt = max_event_of_batch(args.parquet, args.event_column, args.batch_rows, args.batch)
    except ValueError as e:
        die(f"retention-cutoff: {e}")
    except Exception as e:  # pyarrow raises its own error types
        die(f"retention-cutoff: {args.parquet}: {e}", 70)
    now = args.now_s if args.now_s is not None else int(time.time())
    try:
        cutoff, r = retention_window(top, nxt, now)
    except ValueError as e:
        die(f"retention-cutoff: {e}")
    print(json.dumps({"batch": args.batch, "cutoff_s": cutoff, "retention_s": r, "max_event_ns": top,
                      "next_max_event_ns": nxt, "now_s": now}, sort_keys=True))


def hour_index(stamp):
    """Unix hours of a YYYYMMDDTHH key segment (ingest_hour_string,
    crates/ravel-commit/src/keys.rs), the index part ranges count in."""
    t = datetime.datetime.strptime(stamp, "%Y%m%dT%H").replace(tzinfo=datetime.timezone.utc)
    return int(t.timestamp()) // 3600


def bucket_files(keys_lines, tenant_hash, signal_code):
    """{(shard, hour): {filename}} of every key the census lists under one
    tenant and signal's commit prefix,
    t/<hash>/<signal>/c/<shard>/<YYYYMMDDTHH>/<filename> (hour in unix hours)."""
    buckets = {}
    for line in keys_lines:
        key = line.rstrip("\n").split("\t")[-1]
        parts = key.split("/")
        if (len(parts) == 7 and parts[0] == "t" and parts[1] == tenant_hash and parts[2] == signal_code
                and parts[3] == "c"):
            buckets.setdefault((parts[4], hour_index(parts[5])), set()).add(parts[6])
    return buckets


def tombstone_gaps(buckets, first, last):
    """(missing, extra, bare_hours) for a retirement of hours [first, last]:
    the (shard, hour) buckets in the range that hold a commit record (any
    .cmt) and no retire.tmb; the buckets outside it that hold one; and the
    hours in it with no commit-holding bucket at all, where no tombstone
    can be checked."""
    missing, extra, held = [], [], set()
    for (shard, hour), files in sorted(buckets.items()):
        inside = first <= hour <= last
        if inside and any(f.endswith(".cmt") for f in files):
            held.add(hour)
            if "retire.tmb" not in files:
                missing.append((shard, hour))
        if not inside and "retire.tmb" in files:
            extra.append((shard, hour))
    return missing, extra, [h for h in range(first, last + 1) if h not in held]


def cmd_tombstone_check(args):
    """Exit 0 when every (shard, hour) bucket of hours [--first, --last]
    that holds a commit record also holds its retire.tmb, and no bucket
    outside them does; exit 1 naming the buckets otherwise (not yet, or
    more than asked); exit 65 when an hour in the range holds no commit
    record in any shard, which no wait changes."""
    try:
        with open(args.keys) as f:
            buckets = bucket_files(f, args.tenant_hash, args.signal_code)
    except (OSError, ValueError) as e:
        die(f"tombstone-check: {args.keys}: {e}", 70)
    missing, extra, bare = tombstone_gaps(buckets, args.first, args.last)
    tombstoned = sorted(b for b, files in buckets.items() if "retire.tmb" in files)
    print(json.dumps({"tombstoned": [list(b) for b in tombstoned], "missing": [list(b) for b in missing],
                      "extra": [list(b) for b in extra], "want_first": args.first, "want_last": args.last}))
    if bare:
        die(f"tombstone-check: hours {bare} hold no commit record in any shard", 65)
    if missing or extra:
        die(f"tombstone-check: buckets (shard, hour) {missing} hold commit records and no retire.tmb, "
            f"{extra} outside {args.first}..={args.last} hold one")


def maint_records(keys_lines, tenant_hash, signal_code):
    """Every compaction record (l1.<hash16>.cmt) and rewrite record
    (rw.<hash16>.cmt) the census lists for one tenant and signal, as
    {key: (shard, hour)}."""
    out = {}
    for (shard, hour), files in bucket_files(keys_lines, tenant_hash, signal_code).items():
        for f in files:
            if f.endswith(".cmt") and f.startswith(("l1.", "rw.")):
                out[f"{shard}/{hour}/{f}"] = (shard, hour)
    return out


def cmd_maint_records(args):
    """The compaction and rewrite records of one tenant and signal before
    and after a maintain pass, written to --out; exit 1 naming any record
    the pass added, since every such bucket is reconciled and its day part
    re-encoded by the next fold."""
    try:
        with open(args.before) as f:
            before = maint_records(f, args.tenant_hash, args.signal_code)
        with open(args.after) as f:
            after = maint_records(f, args.tenant_hash, args.signal_code)
    except (OSError, ValueError) as e:
        die(f"maint-records: {e}", 70)
    new = sorted(k for k in after if k not in before)
    write_json(args.out, {"tenant_hash": args.tenant_hash, "signal_code": args.signal_code,
                          "before": sorted(before), "after": sorted(after), "new": new,
                          "new_hours": sorted({after[k][1] for k in new})})
    if new:
        die(f"maint-records: the maintain pass wrote {len(new)} compaction or rewrite records "
            f"(shard/hour/file): {new[:10]}")


def git_commit(repo, ref):
    """The full SHA ref names in repo, or None when it names no commit."""
    if not ref:
        return None
    r = subprocess.run(["git", "-C", repo, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 and r.stdout.strip() else None


def git_is_ancestor(repo, ancestor, sha):
    r = subprocess.run(["git", "-C", repo, "merge-base", "--is-ancestor", ancestor, sha], capture_output=True)
    if r.returncode not in (0, 1):
        raise ValueError(f"git merge-base --is-ancestor {ancestor} {sha} exited {r.returncode}")
    return r.returncode == 0


def fold_binary(repo, sha, c1, r2):
    """{sha, carries_c1, carries_r2} of the retired_hour fold binary: whether
    its SHA descends from run.toml's c1_sha and r2_sha. An empty r2_sha is
    a fix not landed yet, so false. Raises when the binary's SHA or c1_sha
    names no commit, or r2_sha is set and names none."""
    full = git_commit(repo, sha)
    if full is None:
        raise ValueError(f"the fold binary's SHA {sha!r} names no commit in {repo}")
    c1_full = git_commit(repo, c1)
    if c1_full is None:
        raise ValueError(f"retired_hour.c1_sha {c1!r} names no commit in {repo}")
    r2_full = None
    if r2:
        r2_full = git_commit(repo, r2)
        if r2_full is None:
            raise ValueError(f"retired_hour.r2_sha {r2!r} names no commit in {repo}")
    return {"sha": full, "c1_sha": c1_full, "r2_sha": r2_full or "",
            "carries_c1": git_is_ancestor(repo, c1_full, full),
            "carries_r2": bool(r2_full) and git_is_ancestor(repo, r2_full, full)}


def cmd_fold_binary(args):
    try:
        doc = fold_binary(args.repo, args.sha, args.c1, args.r2)
    except ValueError as e:
        die(f"fold-binary: {e}")
    write_json(args.out, doc)


def day_parts(doc):
    """Per catalog part of a catalog stamp: its first and last hour (unix
    hours), its UTC day, entry count and leaves (slices) per field."""
    out = []
    for p in doc.get("parts", []):
        lo = hi = None
        a, sep, b = str(p.get("range")).partition("..=")
        if sep and a.isdigit() and b.isdigit():
            lo, hi = int(a), int(b)
        slices = {}
        for leaf in p.get("leaves", []):
            slices[leaf.get("field")] = slices.get(leaf.get("field"), 0) + 1
        out.append({"index": p.get("index"), "min_hour": lo, "max_hour": hi,
                    "day": lo // 24 if lo is not None else None,
                    "one_day": lo is not None and lo // 24 == hi // 24,
                    "entry_count": p.get("entry_count"), "slices": slices})
    return out


def cmd_day_parts_stamp(args):
    """stamps/STATE.LABEL.day_parts.json from the state's catalog stamp: the
    HEAD watermark hour and, per part, min_hour, max_hour, day, entry_count
    and slices per field."""
    src = os.path.join(args.stamps, f"{args.state}.{args.label}.catalog.json")
    try:
        with open(src) as f:
            doc = json.load(f)
    except (OSError, ValueError) as e:
        die(f"day-parts-stamp: {src}: {e}")
    parts = day_parts(doc)
    write_json(os.path.join(args.stamps, f"{args.state}.{args.label}.day_parts.json"),
               {"state": args.state, "label": args.label,
                "watermark_hour": (doc.get("head") or {}).get("watermark_hour"),
                "days": sorted({p["day"] for p in parts if p["day"] is not None}),
                "parts": parts, "part_count": len(parts)})


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


def leaf_slices(doc):
    """{field: the most leaves (key slices) one part of a catalog stamp
    carries for that field}."""
    out = {}
    for part in doc.get("parts", []):
        counts = {}
        for leaf in part.get("leaves", []):
            counts[leaf.get("field")] = counts.get(leaf.get("field"), 0) + 1
        for field, n in counts.items():
            out[field] = max(out.get(field, 0), n)
    return out


def cmd_advance_check(args):
    """The advanced_leaves state's two preconditions (README "States"), read
    from the stamps the run already wrote. (i) fold_advance folded every
    label with the leaf switch on and exited 0: a fold with the switch off
    re-encodes the parts and drops their leaf refs. (ii) The check label's
    check field carries more slices per part in advanced_leaves's catalog
    stamp than in folded_leaves's, the one stamped after the leaf rebuild, so
    the state measures an advanced tail. Prints the slices per (part, field)
    of both stamps as one JSON line; exit 0 when both hold, 1 naming the
    first that does not."""
    folds = {}
    path = os.path.join(args.stamps, "folds.jsonl")
    try:
        with open(path) as f:
            for line in f:
                if line.strip():
                    d = json.loads(line)
                    if d.get("transition") == "fold_advance":
                        folds[d.get("signal")] = d
    except (OSError, ValueError) as e:
        die(f"advance-check: cannot read {path}: {e}")
    for label in args.labels:
        line = folds.get(label)
        if line is None:
            die(f"advance-check: no fold_advance fold stamp for {label}: the mixed state did not fold its tail")
        if line.get("exit") != 0:
            die(f"advance-check: fold_advance on {label} exited {line.get('exit')}")
        if "--key-index-leaves" not in str(line.get("kind") or "").split():
            die(f"advance-check: fold_advance on {label} ran without --key-index-leaves (kind "
                f"{line.get('kind')!r}): a fold with the switch off drops the parts' leaf refs")
    slices = {}
    for state in ("folded_leaves", "advanced_leaves"):
        slices[state] = {}
        for label in args.labels:
            name = os.path.join(args.stamps, f"{state}.{label}.catalog.json")
            try:
                with open(name) as f:
                    slices[state][label] = leaf_slices(json.load(f))
            except (OSError, ValueError) as e:
                die(f"advance-check: cannot read {name}: {e}")
    print(json.dumps({"slices_per_part_field": slices}, sort_keys=True))
    rebuilt = slices["folded_leaves"].get(args.check_label, {}).get(args.check_field)
    advanced = slices["advanced_leaves"].get(args.check_label, {}).get(args.check_field)
    if not rebuilt or not advanced or advanced <= rebuilt:
        die(f"advance-check: {args.check_label} {args.check_field} has {advanced} slices per part in "
            f"advanced_leaves against {rebuilt} after the leaf rebuild: the advance added no slice, so the "
            f"state would not measure an advanced tail")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd_name", required=True)

    ac = sub.add_parser("advance-check")
    ac.add_argument("--stamps", required=True)
    ac.add_argument("--labels", nargs="+", default=["spans", "logs"])
    ac.add_argument("--check-label", default="logs")
    ac.add_argument("--check-field", default="request_id")
    ac.set_defaults(fn=cmd_advance_check)

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
    p.add_argument("--temp", choices=["cold", "inflight", "prefold"], default="cold")
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

    dw = sub.add_parser("day-parts-window")
    dw.add_argument("--days", type=int, required=True)
    dw.add_argument("--now-hour", type=int, help="default: the current unix hour")
    dw.add_argument("--end-day", type=int, help="the last day, in unix days (default: the latest that passes)")
    dw.set_defaults(fn=cmd_day_parts_window)

    mn = sub.add_parser("midnight-truth")
    mn.add_argument("--corpus", required=True, help="the gen_corpus output directory (logs.parquet)")
    mn.add_argument("--truth", required=True, help="that corpus's truth.json, rewritten in place")
    mn.set_defaults(fn=cmd_midnight_truth)

    rc = sub.add_parser("retention-cutoff")
    rc.add_argument("--parquet", required=True)
    rc.add_argument("--event-column", default="ts")
    rc.add_argument("--batch-rows", type=int, required=True)
    rc.add_argument("--batch", type=int, required=True, help="K: the strided load's batch (ingest bucket) to expire")
    rc.add_argument("--now-s", type=int, help="default: the current unix second")
    rc.set_defaults(fn=cmd_retention_cutoff)

    tc = sub.add_parser("tombstone-check")
    tc.add_argument("--keys", required=True, help="census keys file (size<TAB>key per line)")
    tc.add_argument("--tenant-hash", required=True)
    tc.add_argument("--signal-code", default="l")
    tc.add_argument("--first", type=int, required=True, help="first tombstoned hour wanted, unix hours")
    tc.add_argument("--last", type=int, required=True)
    tc.set_defaults(fn=cmd_tombstone_check)

    mr = sub.add_parser("maint-records")
    mr.add_argument("--before", required=True, help="census keys file before the maintain pass")
    mr.add_argument("--after", required=True, help="census keys file after it")
    mr.add_argument("--tenant-hash", required=True)
    mr.add_argument("--signal-code", default="l")
    mr.add_argument("--out", required=True)
    mr.set_defaults(fn=cmd_maint_records)

    fb = sub.add_parser("fold-binary")
    fb.add_argument("--repo", required=True)
    fb.add_argument("--sha", required=True)
    fb.add_argument("--c1", required=True)
    fb.add_argument("--r2", default="")
    fb.add_argument("--out", required=True)
    fb.set_defaults(fn=cmd_fold_binary)

    dps = sub.add_parser("day-parts-stamp")
    dps.add_argument("--stamps", required=True)
    dps.add_argument("--state", required=True)
    dps.add_argument("--label", required=True)
    dps.set_defaults(fn=cmd_day_parts_stamp)

    kv = sub.add_parser("kv-report")
    kv.add_argument("--log", required=True)
    kv.set_defaults(fn=cmd_kv_report)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
