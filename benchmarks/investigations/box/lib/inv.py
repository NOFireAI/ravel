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
                            typed-attr-column show
  needle-blocks             storage blocks holding a needle, from rlog
                            inspect's field directory
  stamp                     write or append one stamp document
  get                       print one run.toml value for bash
  warmup                    wait for the server's startup cache warmup and
                            print what it warmed

Every failure exits non-zero with one line on stderr.
"""

import argparse
import datetime
import hashlib
import hmac
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


def object_class(key):
    """The census class of a key, by its layout path (docs/catalog-and-mvcc.md)."""
    parts = key.split("/")
    if parts[0] != "t" or len(parts) < 3:
        return parts[0]
    rest = parts[2:]
    if not rest:
        return "tenant"
    head = rest[0]
    if head in ("logs", "spans", "metrics") and len(rest) > 1:
        return f"{head}/{rest[1]}"
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


SIGNAL_DIR = {"s": "spans", "l": "logs", "m": "metrics"}


def tenant_census(keys, tenant_hash):
    """One tenant's objects: classes by layout path, each signal's data objects
    with its largest (the object the per-state inspect stamps read), and every
    catalog object's size (HEAD, snapshot parts, key-index leaves)."""
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
            d = data.setdefault(SIGNAL_DIR[rest[0]], {"objects": 0, "bytes": 0, "largest": None, "largest_bytes": -1})
            d["objects"] += 1
            d["bytes"] += size
            if size > d["largest_bytes"] or (size == d["largest_bytes"] and key < d["largest"]):
                d["largest"], d["largest_bytes"] = key, size
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
    doc["entries"] = sum(p["entry_count"] or 0 for p in doc["parts"])
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


def cmd_needle_blocks(args):
    """Storage blocks holding a needle, from rlog inspect's field directory:
    the tag column is null on every row but a needle row, so its
    present_blocks summed over the logs objects is the count gen_corpus
    predicts in units of its own row order (truth needle_blocks.any)."""
    with open(args.census) as f:
        census = json.load(f)
    tc = census["tenants"][args.tenant]
    prefix = f"t/{tc['hash']}/l/"
    keys = []
    with open(args.keys) as f:
        for line in f:
            size, _, key = line.rstrip("\n").partition("\t")
            if key.startswith(prefix) and key.endswith(".rseg"):
                keys.append(key)
    keys.sort()
    picked = keys if args.max_objects <= 0 else keys[: args.max_objects]
    tot = {"tag_present_blocks": 0, "block_count": 0, "tag_null_count": 0, "record_count": 0}
    for key in picked:
        obj = parse_object_inspect(run_cli(cli_base(args) + ["rlog", "inspect", key], stop_at="records ("))
        tot["block_count"] += obj.get("block_count") or 0
        tot["record_count"] += obj.get("record_count") or 0
        tag = next((c for c in obj["field_dir"] if c.get("name") == "tag"), None)
        if tag:
            tot["tag_present_blocks"] += tag.get("present_blocks") or 0
            tot["tag_null_count"] += tag.get("null_count") or 0
    with open(args.truth) as f:
        truth = json.load(f)
    write_json(args.out, {"tenant": args.tenant, "objects_total": len(keys), "objects_inspected": len(picked),
                          **tot, "truth_needle_blocks": truth["needle_blocks"], "truth_needles": truth["needles"],
                          "truth_block_rows": truth["generator"]["block_rows"]})


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
            line = {**base, "temp": temp, "try": i, "truth_key": prefix + fill(st["truth_key"], p)}
            if st.get("oracle"):
                line["oracle"] = st["oracle"]
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


def proc_cpu_s(pid):
    """utime + stime of pid in seconds, from /proc/<pid>/stat."""
    with open(f"/proc/{pid}/stat") as f:
        stat = f.read()
    fields = stat[stat.rindex(")") + 2 :].split()
    ticks = os.sysconf("SC_CLK_TCK")
    return (int(fields[11]) + int(fields[12])) / ticks


def proc_hwm_bytes(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
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
    except OSError as e:
        code, raw = None, str(e).encode()
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
        "server_cpu_s": round(cpu1 - cpu0, 6),
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
        line["outcome"] = "unsupported-on-this-build"
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
    append_line(args.calls, line)
    if line["outcome"] != "ok":
        # The error is on record for evaluate.py; only a server that did not
        # answer at all stops the run.
        print(f"{plan['statement']} ({plan['state']}, {plan['temp']} try {plan['try']}): "
              f"HTTP {code}: {line['error'][:200]}", file=sys.stderr)
        if code is None:
            sys.exit(1)


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
    one JSON document (`--json`), else the last line carrying a JSON object
    (load's `load report json : {...}`)."""
    try:
        doc = json.loads(text)
        if isinstance(doc, dict):
            return doc
    except ValueError:
        pass
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

    kv = sub.add_parser("kv-report")
    kv.add_argument("--log", required=True)
    kv.set_defaults(fn=cmd_kv_report)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
