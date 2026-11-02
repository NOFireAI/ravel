#!/usr/bin/env bash
# Run one MetricsBench profile end to end against this directory's compose
# stack (the comparators plus the ravel.yml overlay), stamp what ran, and fail
# loudly (ADR-0927, epic #2821).
#
# Usage:
#   run.sh --profile NAME
#     (--ravel-image REF@sha256:DIGEST | --ravel-image REF --ravel-image-id sha256:ID)
#     --ravel-source-commit SHA --bin-dir DIR --out-dir DIR
#     [--anchor-ms N] [--passes N] [--reps N] [--keep]
#     [--rss-band CONTAINER:MIN_BYTES:MAX_BYTES]...
#     [--ravel-objects-band MIN:MAX]
#     [--diagnostic-prometheus-max-samples N] [--folded-catalog-get-max-fraction F]
#     [--folded-catalog-get-floor N]
#     [--real-s3-bucket-prefix PREFIX --real-s3-region REGION]
#
# The Ravel image comes in one of two forms, and stamps.json says which ran
# (ravel_image.kind):
#
#   published    --ravel-image REF@sha256:DIGEST, a registry image pinned by
#                its digest. The only form a comparable run may use.
#   local-build  --ravel-image REF --ravel-image-id sha256:ID, an image built
#                on this host (it has no registry digest), named by a local
#                tag and identified by its content: `docker image inspect
#                REF` must report Id equal to ID. Accepted with --profile ci
#                only, the nightly's non-comparable profile; a local-build
#                run is never publishable.
#
# Steps, in order; the first failure stops the run, except a failed ingest
# verification (see ingest):
#
#   (refusals)  a --ravel-image without an @sha256: digest and no
#               --ravel-image-id, a --ravel-image-id that is not sha256:<64
#               hex>, given with a profile other than ci, or not the Id
#               `docker image inspect` reports for --ravel-image, a bin
#               missing from --bin-dir, an out-dir that already holds a run's output, a
#               profile the workload does not declare, an off-grid anchor, or
#               a metricsbench compose project (containers or volumes) left
#               from an earlier run, a Docker engine arch (docker version
#               Server.Arch, else uname -m when the engine does not answer)
#               other than amd64/x86_64 or arm64/aarch64, or neither
#               sha256sum nor shasum on PATH; nothing is started
#   stamps      <out>/stamps.json, written before the first step and completed
#               at the end, whatever the outcome
#   fetch-s3s-fs  the s3s-fs binary the s3s service runs (ADR-0927 amendment
#               2026-10-29): the cached copy for the engine's arch if its
#               sha256 equals the pinned constant below, else the release asset
#               downloaded and checked against it; any other sha256 is
#               refused with exit 65 before anything starts. Its path is
#               exported as S3S_FS_BIN and stamped (object_store)
#   up          docker compose up -d of docker-compose.yml plus ravel.yml,
#               RAVEL_IMAGE and S3S_FS_BIN set
#   qualify     the one-shots createbuckets and ravel-qualify exited 0
#   image       the Ravel image as docker image inspect resolves it: a
#               published image must list the digest the reference names, a
#               local build must still have the Id --ravel-image-id names
#   ready       every engine answers its readiness URL
#   sampler     a background loop appends each compose container's memory use
#               (docker stats --no-stream) to <out>/memory.jsonl, one sample
#               every SAMPLE_INTERVAL seconds after the previous one ends
#   ingest      metricsbench_ingest into all four portable endpoints, read back
#               through their query URLs, into <out>/ingest.json; Ravel's
#               bearer token goes through --ravel-bearer-token-env. When
#               the bin exits non-zero and ingest.json still holds a row
#               whose read-back failed or that stalled, ingest is stamped
#               failed (failed_step) and the run goes on through every later
#               step as a diagnostic: query gets
#               --diagnostic-after-failed-ingest, validate must refuse the
#               report it writes, and the run exits 70 naming ingest. A
#               later step that fails is listed in failed_after_ingest and
#               ends the run there, with the same exit; that includes query
#               itself, which refuses when the failed row is the Prometheus
#               oracle's (no report.json, no validate or memory step).
#               Any other ingest failure stops the run
#   mimir-readiness  the ingest row for mimir read back what it accepted
#               (after a failed ingest verification, stamped and not checked)
#   objects     Ravel's bucket (ravel-metricsbench through http://s3s:9000, or
#               <PREFIX>-ravel on the regional AWS endpoint in the real-S3
#               lane) listed by aws s3api list-objects-v2 in the createbuckets
#               service with its entrypoint overridden, every page; the keys
#               counted and summed by kind into <out>/ravel-objects.json and
#               stamped (ravel_objects): data (t/*/m/l0/*.rseg), commit
#               (t/*/m/c/), qualify (sys/qualify/), other, and total. A
#               failed or truncated listing fails the step. With
#               --ravel-objects-band MIN:MAX the data count, and only it,
#               must lie in [MIN, MAX]; otherwise the step fails
#   capture    capture-effective-config.sh into <out>/effective-config.json
#   provenance  metricsbench_report provenance into <out>/provenance.json
#   fold-state  ravel_catalog_fold_last_success_timestamp_seconds from Ravel's
#               /metrics, stamped
#   query       metricsbench_query against all four engines into
#               <out>/report.json, with the per-entry ULP gaps and oracle
#               retries in <out>/diagnostics.json
#   validate    metricsbench_report validate <out>/report.json
#   memory      the sampler stopped, <out>/memory-summary.json written (peak
#               and final bytes per container, OOMKilled from docker inspect),
#               every --rss-band checked. In a full run a failure here is
#               stamped in deferred_failures and the run goes on to the four
#               steps below before it exits (see after the list)
#   stop        docker compose stop ravel-server: the fold below runs with
#               --writers-stopped, which ADR-2677 decision 1 makes safe only
#               once the writer has exited. Stamped as ravel_restart
#   fold        ravel-cli catalog fold --signal metrics --writers-stopped, run
#               through `docker compose run` on the ravel-qualify service (so
#               it reads the same store as the server in either lane), into
#               <out>/fold.out. Fails unless seal_through_hour is Some(N) and
#               watermark_hour is at or after the hour the ingest step ended
#               in. Stamped as ravel_catalog_fold_writers_stopped. A failed
#               fold ends the run with the server still stopped
#   start       docker compose start ravel-server, then its readiness URL
#               answers 200 as in ready. Stamped as ravel_restart
#   readback-folded  Ravel's /metrics scraped into
#               <out>/ravel-metrics-before-folded-readback.txt,
#               metricsbench_ingest --readback-only for the Ravel row into
#               <out>/readback-folded.json, /metrics scraped again into
#               <out>/ravel-metrics-after-folded-readback.txt. Fails on a
#               non-zero exit or a missing scrape. Stamped as readback_folded
#               with the GET and commit-record cache deltas between the scrapes.
#               Also fails when the GETs the fetch cache did not cause
#               (catalog_get_delta) exceed --folded-catalog-get-floor
#               (default 64) plus --folded-catalog-get-max-fraction
#               (default 0.05) times the data objects the objects step listed
#               (data_objects_listed, data.count in <out>/ravel-objects.json),
#               or when either figure cannot be read. A limit at or above
#               data_objects_listed cannot tell a folded read from an
#               unfolded one; that is stamped not discriminating and passes.
#               The flush counters' sum in <out>/ravel-metrics-at-query.txt
#               is stamped beside it as data_objects_flushed, a cross-check
#               the limit does not use
#
# stop, fold, start and readback-folded also run after a failed ingest
# verification, when the Ravel row itself has no row_failure and did not
# stall; a failure there is listed in failed_after_ingest. Otherwise the four
# are skipped and their stamps say why.
#
# A failed memory step (an --rss-band breach or an OOM-killed container) does
# not stop them: it is listed in stamps.json as deferred_failures, the four run
# as above, and the run then exits for the memory failure (75 naming memory, or
# 70 naming ingest with memory in failed_after_ingest after a failed ingest
# verification). A folded step that fails after it exits 70 naming that step
# instead, as without the memory failure.
#
# With --diagnostic-prometheus-max-samples N the run is the falsification run
# pre-registered on #2821 instead: steps override (a compose override in the
# out-dir setting Prometheus' --query.max-samples=N), up (Prometheus alone),
# ready, sampler, ingest (Prometheus only), falsification (mb_long_range_7d_avg
# once as an instant query at the newest sample, into
# <out>/falsification.json) and memory. It writes no report.json, and
# stamps.json says diagnostic: true. --ravel-image and --ravel-source-commit
# are optional there, since no Ravel is started; objects does not run
# (ravel_objects is stamped not applicable) and --ravel-objects-band is a
# usage error.
#
# With --real-s3-bucket-prefix and --real-s3-region the run is the real-S3 lane
# (ADR-0927 decision 10): the overlay real-s3.yml moves Ravel and Mimir onto the
# operator's <PREFIX>-ravel, <PREFIX>-mimir-blocks, <PREFIX>-mimir-ruler and
# <PREFIX>-mimir-alertmanager buckets with the instance role's credentials.
# fetch-s3s-fs does not run and s3s does not start; createbuckets instead fails
# the qualify step when a bucket is missing or holds an object. stamps.json and
# the provenance name real S3. It takes a published image only.
#
# The stack is always torn down with `docker compose ... down -v` (after its
# logs, s3s-fs's among them when it ran, are saved to <out>/compose.log) unless
# --keep is given. That removes no real-S3 bucket or object.
#
# Exit codes:
#   0   every step passed
#   64  usage error, including --ravel-image-id with a digest-pinned
#       --ravel-image (both forms) or with no --ravel-image, and no Ravel
#       image at all on a run that starts Ravel (neither form)
#   65  a refused input (see refusals above), or an s3s-fs binary whose
#       sha256 is not the pinned one (step fetch-s3s-fs); nothing was
#       started
#   70  a step failed; its name is in the message and in stamps.json
#       (failed_step). After a failed ingest verification this is ingest,
#       even when a later step failed or the memory bands were violated
#   75  a --rss-band was violated or a container was OOM-killed, and no
#       later step failed
#
# Environment, for tests: METRICSBENCH_DOCKER (default docker),
# METRICSBENCH_CAPTURE (default capture-effective-config.sh beside this
# script), METRICSBENCH_SAMPLE_INTERVAL (default 5), METRICSBENCH_READY_TRIES
# and METRICSBENCH_READY_INTERVAL (default 300 tries 2 s apart; a try that
# reaches a port which does not answer also spends up to 5 s in curl, so the
# bound per engine is tries x (interval + 5) s),
# METRICSBENCH_IMDS (default http://169.254.169.254),
# METRICSBENCH_S3S_FS_CACHE (default ${XDG_CACHE_HOME:-~/.cache}/ravel-metricsbench,
# where fetch-s3s-fs keeps the verified binary between runs).
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/../.." && pwd)"

DOCKER="${METRICSBENCH_DOCKER:-docker}"
CAPTURE="${METRICSBENCH_CAPTURE:-${here}/capture-effective-config.sh}"
SAMPLE_INTERVAL="${METRICSBENCH_SAMPLE_INTERVAL:-5}"
READY_TRIES="${METRICSBENCH_READY_TRIES:-300}"
READY_INTERVAL="${METRICSBENCH_READY_INTERVAL:-2}"
IMDS="${METRICSBENCH_IMDS:-http://169.254.169.254}"
PROJECT="${COMPOSE_PROJECT_NAME:-metricsbench}"
S3S_FS_CACHE="${METRICSBENCH_S3S_FS_CACHE:-${XDG_CACHE_HOME:-${HOME}/.cache}/ravel-metricsbench}"

# The s3s-fs build the s3s service runs, pinned by the sha256 of each arch's
# release asset. fetch_s3s_fs refuses any binary, cached or downloaded, whose
# sha256 differs; tests/every_comparator_pins_an_image_digest.sh (category 7)
# fails if a constant or that comparison goes.
S3S_FS_RELEASE="s3s-fs-0.17.0-ravel.3"
S3S_FS_URL_BASE="https://github.com/NOFireAI/s3s/releases/download/${S3S_FS_RELEASE}"
S3S_FS_SHA256_X86_64="99592a8a64cbb773c34aee9966537eb4d1091d9a91b70699220f1f34dd4b384e"
S3S_FS_SHA256_AARCH64="92d7bbbb29bd4c5aee00b5f1c280c1402e799ed8a28f497e5ba813f1ebc3eecc"

WORKLOAD="${repo_root}/benchmarks/metrics/workload.json"
CORPUS="${repo_root}/benchmarks/metrics/metrics.corpus.json"

# The fixed local-development token ravel.yml's --tenant-token sets. It
# reaches the bins through this variable's name, never through argv.
RAVEL_TOKEN_ENV="METRICSBENCH_RAVEL_TOKEN"
RAVEL_TOKEN_VALUE="metricsbench-token"
RAVEL_TENANT="metricsbench"
# ravel.yml's --shards; the fold must name the same count.
RAVEL_SHARDS=4
# metricsbench_ingest's --readback-settle-secs default, which the ingest step
# runs with; the folded read-back passes it so both reads settle alike.
READBACK_SETTLE_SECS=60

PROM_BASE="http://127.0.0.1:9090"
VM_BASE="http://127.0.0.1:8428"
MIMIR_BASE="http://127.0.0.1:9009"
RAVEL_BASE="http://127.0.0.1:4318"

FALSIFICATION_ENTRY="mb_long_range_7d_avg"

EXIT_USAGE=64
EXIT_REFUSED=65
EXIT_STEP=70
EXIT_MEMORY=75

say() {
  echo "metricsbench-run: $*" >&2
}

usage() {
  sed -n '7,15p' "$0" >&2
}

usage_error() {
  say "$*"
  usage
  exit "$EXIT_USAGE"
}

refuse() {
  say "refused: $*"
  exit "$EXIT_REFUSED"
}

utc_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# The JSON work, in one place: python3 -I with the op as its first argument.
read -r -d '' PYLIB <<'PY'
import json, os, re, sys, time

def load(path):
    with open(path) as f:
        return json.load(f)

def dump(path, doc):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)

def set_path(doc, dotted, value):
    keys = dotted.split(".")
    cur = doc
    for k in keys[:-1]:
        if not isinstance(cur.get(k), dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value

UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4,
         "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4}

def parse_bytes(text):
    m = re.fullmatch(r"\s*([0-9.]+)\s*([A-Za-z]+)\s*", text)
    if not m or m.group(2).lower() not in UNITS:
        return None
    return int(float(m.group(1)) * UNITS[m.group(2).lower()])

def inspect_rows(path):
    try:
        rows = load(path)
    except (OSError, ValueError):
        return []
    return rows if isinstance(rows, list) else []

op, args = sys.argv[1], sys.argv[2:]

if op == "set":
    path, dotted, raw = args
    doc = load(path)
    set_path(doc, dotted, json.loads(raw))
    dump(path, doc)
elif op == "setstr":
    path, dotted, raw = args
    doc = load(path)
    set_path(doc, dotted, raw)
    dump(path, doc)
elif op == "append":
    path, dotted, raw = args
    doc = load(path)
    cur = doc
    for k in dotted.split("."):
        cur = cur[k]
    cur.append(raw)
    dump(path, doc)
elif op == "get":
    path, dotted = args
    cur = load(path)
    for k in dotted.split("."):
        cur = cur[k]
    print(cur)
elif op == "profile-grid":
    workload, name = args
    for p in load(workload).get("profiles", []):
        if p.get("name") == name:
            print(p["scrape_interval_secs"], p["duration_secs"])
            sys.exit(0)
    sys.exit(3)
elif op == "image-inspect":
    ref = args[0]
    rows = json.load(sys.stdin)
    row = rows[0] if isinstance(rows, list) and rows else {}
    digests = row.get("RepoDigests") or []
    want = ref.split("@", 1)[1] if "@" in ref else None
    out = {"reference": ref, "image_id": row.get("Id"), "repo_digests": digests,
           "digest_matches_reference": bool(want) and any(d.endswith("@" + want) for d in digests)}
    print(json.dumps(out))
    sys.exit(0 if out["digest_matches_reference"] else 3)
elif op == "image-inspect-local":
    ref, want = args
    rows = json.load(sys.stdin)
    row = rows[0] if isinstance(rows, list) and rows else {}
    out = {"reference": ref, "image_id": row.get("Id"), "repo_digests": row.get("RepoDigests") or [],
           "id_matches": row.get("Id") == want}
    print(json.dumps(out))
    sys.exit(0 if out["id_matches"] else 3)
elif op == "one-shot-exit":
    # docker inspect rows on stdin; prints "<status> <exit code>".
    rows = json.load(sys.stdin)
    if not rows:
        sys.exit(3)
    state = rows[0].get("State", {})
    print(state.get("Status"), state.get("ExitCode"))
elif op == "mem-parse":
    # docker stats --format '{{json .}}' lines on stdin, one JSONL line out each.
    ts = args[0]
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except ValueError:
            continue
        used = (row.get("MemUsage") or "").split("/")[0]
        print(json.dumps({"ts": ts, "container": row.get("Name"),
                          "mem_bytes": parse_bytes(used), "mem_usage": row.get("MemUsage")}))
elif op == "summary":
    # memory.jsonl, docker inspect JSON of every compose container, the
    # summary path, the sample interval, then each band as NAME:MIN:MAX.
    # Exit 0 clean, 3 when a band is violated or a container was OOM-killed.
    samples_path, inspect_path, out_path, interval = args[:4]
    bands = args[4:]
    containers = {}
    count = 0
    if os.path.exists(samples_path):
        with open(samples_path) as f:
            for line in f:
                try:
                    s = json.loads(line)
                except ValueError:
                    continue
                if s.get("mem_bytes") is None or not s.get("container"):
                    continue
                count += 1
                c = containers.setdefault(s["container"], {"service": None, "samples": 0,
                                                           "peak_bytes": 0, "final_bytes": None,
                                                           "oom_killed": None})
                c["samples"] += 1
                c["peak_bytes"] = max(c["peak_bytes"], s["mem_bytes"])
                c["final_bytes"] = s["mem_bytes"]
    oom = []
    for row in inspect_rows(inspect_path):
        name = (row.get("Name") or "").lstrip("/")
        c = containers.setdefault(name, {"service": None, "samples": 0, "peak_bytes": None,
                                         "final_bytes": None, "oom_killed": None})
        c["service"] = ((row.get("Config") or {}).get("Labels") or {}).get("com.docker.compose.service")
        c["oom_killed"] = bool((row.get("State") or {}).get("OOMKilled"))
        if c["oom_killed"]:
            oom.append(name)
    band_rows = []
    for spec in bands:
        name, lo, hi = spec.rsplit(":", 2)
        lo, hi = int(lo), int(hi)
        match = [k for k, c in containers.items() if k == name or c["service"] == name]
        peaks = [containers[k]["peak_bytes"] for k in match if containers[k]["samples"]]
        if len(peaks) != 1:
            verdict = "no_samples" if not peaks else "ambiguous"
            peak = None
        else:
            peak = peaks[0]
            verdict = "above" if peak > hi else "below" if peak < lo else "ok"
        band_rows.append({"container": name, "min_bytes": lo, "max_bytes": hi,
                          "peak_bytes": peak, "verdict": verdict})
    violated = [b for b in band_rows if b["verdict"] != "ok"]
    verdict = "ok"
    if oom:
        verdict = "oom_killed"
    elif violated:
        verdict = "band_violated"
    dump(out_path, {"sample_interval_secs": float(interval), "samples": count,
                    "containers": containers, "oom_killed": oom, "bands": band_rows,
                    "verdict": verdict})
    for b in violated:
        print(f"--rss-band {b['container']}:{b['min_bytes']}:{b['max_bytes']} {b['verdict']} "
              f"(peak {b['peak_bytes']})")
    for name in oom:
        print(f"container {name} was OOM-killed")
    sys.exit(3 if verdict != "ok" else 0)
elif op == "mimir-readiness":
    for row in load(args[0]).get("systems", []):
        if row.get("system") != "mimir":
            continue
        rb = row.get("readback") or {}
        if rb.get("matches_expected") is True:
            print(f"ingest read-back for mimir matched (series_count {rb.get('series_count')}, "
                  f"sample_count {rb.get('sample_count')}): the replayed window is visible "
                  f"before the first statement")
            sys.exit(0)
        print(f"ingest read-back for mimir did not match: {rb.get('mismatches')}")
        sys.exit(3)
    print("the ingest report has no mimir row")
    sys.exit(3)
elif op == "ingest-verification-failed":
    # One line naming every row of an ingest report whose read-back failed or
    # that stalled; exit 3 when the file is not such a report. metricsbench_ingest
    # prints a report on a failed exit only for those two failures.
    try:
        doc = load(args[0])
    except (OSError, ValueError):
        sys.exit(3)
    if not isinstance(doc, dict):
        sys.exit(3)
    failed = []
    for row in doc.get("systems") or []:
        rb = row.get("readback")
        if row.get("readback_error"):
            failed.append(f"{row.get('system')}: read-back failed ({row['readback_error']})")
        elif isinstance(rb, dict) and rb.get("matches_expected") is False:
            failed.append(f"{row.get('system')}: read-back did not match ({'; '.join(rb.get('mismatches') or [])})")
    for st in doc.get("stalled") or []:
        failed.append(f"{st.get('system')}: stalled ({st.get('error')})")
    if not failed:
        sys.exit(3)
    print("; ".join(failed))
elif op == "capture-images":
    # Each comparator's image block as the capture recorded it.
    comps = load(args[0]).get("comparators", {})
    print(json.dumps({name: c.get("image") if isinstance(c, dict) else c
                      for name, c in comps.items()}))
elif op == "capture-pins":
    # One name=version=sha256:... line per comparator other than Ravel.
    comps = load(args[0]).get("comparators", {})
    bad = []
    for name in ("prometheus", "victoriametrics", "mimir"):
        c = comps.get(name) or {}
        version = c.get("version")
        image = c.get("image")
        digests = image.get("repo_digests") if isinstance(image, dict) else None
        digest = None
        for d in digests or []:
            if "@sha256:" in d:
                digest = d.split("@", 1)[1]
                break
        if not isinstance(version, str) or not version or not digest:
            bad.append(name)
            continue
        print(f"{name}={version}={digest}")
    if bad:
        print("no version or image digest captured for " + ", ".join(bad), file=sys.stderr)
        sys.exit(3)
elif op == "fold-gauge":
    gauge = "ravel_catalog_fold_last_success_timestamp_seconds"
    with open(args[0]) as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 2 and (parts[0] == gauge or parts[0].startswith(gauge + "{")):
                print(parts[1])
                sys.exit(0)
    sys.exit(3)
elif op == "ravel-row-state":
    # Exit 0 when the ingest report's Ravel row replayed: present, no
    # row_failure, not stalled, not a failed replay. Otherwise exit 3 with why.
    try:
        doc = load(args[0])
    except (OSError, ValueError):
        print("ingest.json is not readable")
        sys.exit(3)
    rows = [r for r in doc.get("systems") or [] if r.get("system") == "ravel"]
    if not rows:
        print("the ingest report has no ravel row")
        sys.exit(3)
    rf = rows[0].get("row_failure")
    if rf:
        print(f"the ravel row has a row_failure ({rf.get('error') if isinstance(rf, dict) else rf})")
        sys.exit(3)
    for st in doc.get("stalled") or []:
        if st.get("system") == "ravel":
            print(f"the ravel row stalled ({st.get('error')})")
            sys.exit(3)
    for fr in doc.get("failed_replays") or []:
        if fr.get("system") == "ravel":
            print(f"the ravel row's replay failed ({fr.get('error')})")
            sys.exit(3)
elif op == "fold-stamp":
    seal, watermark, entries, buckets, no_op, end_hour, precondition = args
    def hour(v):
        m = re.fullmatch(r"Some\((\d+)\)", v)
        return int(m.group(1)) if m else v
    def num(v):
        return int(v) if re.fullmatch(r"\d+", v) else v
    print(json.dumps({"seal_through_hour": hour(seal), "watermark_hour": hour(watermark),
                      "entry_count": num(entries), "buckets_folded": num(buckets),
                      "no_op": {"true": True, "false": False}.get(no_op, no_op or None),
                      "ingest_end_hour": int(end_hour), "precondition": precondition}))
elif op == "readback-folded":
    # The bin's stdout, its exit code, the two /metrics scrapes ("" when one
    # failed), the at-query scrape, ravel-objects.json, the catalog GET
    # fraction and floor. Prints the stamp; exit 3 when stdout is not its
    # report.
    out_path, code, before, after, at_query, objects_path, fraction, floor = args
    def metric(path, name, labels):
        if not path or not os.path.isfile(path):
            return None
        total, seen = 0, False
        with open(path) as f:
            for line in f:
                m = re.match(r"^([A-Za-z_:][A-Za-z0-9_:]*)(\{([^}]*)\})?\s+(\S+)", line)
                if not m or m.group(1) != name:
                    continue
                got = dict(re.findall(r'([A-Za-z_][A-Za-z0-9_]*)="([^"]*)"', m.group(3) or ""))
                if all(got.get(k) == v for k, v in labels.items()):
                    total += float(m.group(4))
                    seen = True
        return total if seen else None
    def delta(name, labels):
        a, b = metric(before, name, labels), metric(after, name, labels)
        return None if a is None or b is None else int(b - a)
    stamp = {"exit_code": int(code), "matches_expected": None, "error": None, "series_query_ms": None,
             "store_get_calls_delta": delta("ravel_store_calls_total", {"op": "get"}),
             "commit_record_cache_entries_delta": delta("ravel_catalog_record_cache_entries", {"cache": "commit"})}
    # Store GETs the fetch cache did not cause: on an unfolded read-back these
    # are about one commit-record GET per data object (#2821).
    fetch_misses = delta("ravel_cache_misses_total", {"cache": "fetch"})
    gets = stamp["store_get_calls_delta"]
    stamp["fetch_cache_misses_delta"] = fetch_misses
    stamp["catalog_get_delta"] = None if gets is None or fetch_misses is None else gets - fetch_misses
    # The denominator: the .rseg data objects the objects step listed, the
    # count the fraction was calibrated on.
    listed, listed_error = None, None
    try:
        data = load(objects_path).get("data")
        n = data.get("count") if isinstance(data, dict) else None
        if isinstance(n, int) and not isinstance(n, bool) and n >= 0:
            listed = n
        else:
            listed_error = "no data.count in ravel-objects.json"
    except (OSError, ValueError, AttributeError) as e:
        listed_error = f"ravel-objects.json could not be read ({type(e).__name__})"
    stamp["data_objects_listed"] = listed
    # A cross-check only, not the denominator.
    # One data object per flush: shard.rs flush_tenant calls record_flush once
    # per flush it opens (crates/ravel-ingest/src/shard.rs:2662), and
    # record_flush bumps exactly one of these by its FlushTrigger
    # (crates/ravel-ingest/src/metrics.rs:957-963), so the five are disjoint.
    # Sub-counts of them (ravel_ingest_rerouted_flushes_total,
    # ravel_ingest_grace_extended_stale_flushes_total) are not added.
    # flushes_manual (FlushNow and the shutdown drains, metrics.rs module
    # docs) is left out; the at-query scrape precedes the stop. These are
    # attempt-time, so an abandoned flush counts too.
    flushed, absent = 0, []
    for reason in ("size", "age", "age_adaptive", "age_floor", "arrival"):
        name = f"ravel_ingest_flushes_by_{reason}_total"
        v = metric(at_query, name, {"signal": "metrics"})
        if v is None:
            absent.append(name)
        else:
            flushed += int(v)
    stamp["data_objects_flushed"] = None if absent else flushed
    stamp["folded_catalog_get_max_fraction"] = fraction
    stamp["folded_catalog_get_floor"] = int(floor)
    stamp["folded_catalog_get_limit"] = None
    cat = stamp["catalog_get_delta"]
    if cat is None:
        missing = [n for n, v in (("ravel_store_calls_total{op=\"get\"}", gets),
                                  ("ravel_cache_misses_total{cache=\"fetch\"}", fetch_misses)) if v is None]
        stamp["catalog_get_verdict"] = ("could not measure: no " + " or ".join(missing)
                                        + " delta between the folded read-back's scrapes")
    elif listed is None:
        stamp["catalog_get_verdict"] = f"could not measure: {listed_error}"
    else:
        from fractions import Fraction
        # The floor is the read's fixed catalog cost (HEAD, snapshot parts,
        # commit-token keys, the server's own background GETs in the window).
        limit = int(floor) + Fraction(fraction) * listed
        shown = int(limit) if limit.denominator == 1 else float(limit)
        stamp["folded_catalog_get_limit"] = shown
        # An unfolded read costs about one catalog GET per data object.
        if limit >= listed:
            stamp["catalog_get_verdict"] = (f"not discriminating: limit {shown} is at or above the {listed} "
                                            "data objects listed, so a folded and an unfolded read cannot "
                                            "be told apart at this profile")
        elif cat > limit:
            stamp["catalog_get_verdict"] = (f"breach: catalog_get_delta {cat} exceeds {floor} + {fraction} x "
                                            f"data_objects_listed {listed} = {shown}")
        else:
            stamp["catalog_get_verdict"] = "within"
    try:
        doc = load(out_path)
    except (OSError, ValueError) as e:
        stamp["error"] = f"readback-folded.json is not JSON: {type(e).__name__}"
        print(json.dumps(stamp))
        sys.exit(3)
    rb = doc.get("readback") if isinstance(doc, dict) else None
    if isinstance(rb, dict):
        stamp["matches_expected"] = rb.get("matches_expected")
        if rb.get("matches_expected") is not True:
            stamp["error"] = "; ".join(rb.get("mismatches") or []) or None
        qms = rb.get("query_ms")
        if isinstance(qms, dict):
            stamp["series_query_ms"] = qms.get("series")
    elif isinstance(doc, dict) and doc.get("readback_error"):
        stamp["error"] = doc["readback_error"]
    else:
        stamp["error"] = "readback-folded.json carries neither readback nor readback_error"
        print(json.dumps(stamp))
        sys.exit(3)
    print(json.dumps(stamp))
elif op == "override":
    # docker compose config --format json on stdin; Prometheus' command with
    # --query.max-samples replaced by N, as a compose override document.
    n = args[0]
    cmd = json.load(sys.stdin)["services"]["prometheus"]["command"]
    hits = [i for i, a in enumerate(cmd) if a.startswith("--query.max-samples=")]
    if len(hits) != 1:
        print(f"expected one --query.max-samples in Prometheus' command, found {len(hits)}",
              file=sys.stderr)
        sys.exit(3)
    cmd = list(cmd)
    cmd[hits[0]] = f"--query.max-samples={n}"
    print(json.dumps({"services": {"prometheus": {"command": cmd}}}, indent=2))
elif op == "ravel-objects":
    # aws s3api list-objects-v2 JSON (every page merged), the output path, the
    # bucket, the endpoint, then MIN:MAX or "". Exit 0 clean, 3 when the data
    # count is outside the band, 4 when the listing cannot be counted.
    listing_path, out_path, bucket, endpoint, band = args
    try:
        listing = load(listing_path)
    except (OSError, ValueError) as e:
        print(f"the listing is not JSON: {type(e).__name__}")
        sys.exit(4)
    if not isinstance(listing, dict):
        print("the listing is not a list-objects-v2 document")
        sys.exit(4)
    if listing.get("NextToken") or listing.get("NextContinuationToken") or listing.get("IsTruncated"):
        print("the listing is truncated: it carries a continuation token")
        sys.exit(4)
    rows = listing.get("Contents") or []
    kinds = {k: {"count": 0, "bytes": 0} for k in ("data", "commit", "qualify", "other")}
    total = {"count": 0, "bytes": 0}
    for row in rows:
        key, size = (row.get("Key"), row.get("Size")) if isinstance(row, dict) else (None, None)
        if not isinstance(key, str) or not isinstance(size, int):
            print(f"a listing row has no Key or integer Size: {row!r}")
            sys.exit(4)
        total["count"] += 1
        total["bytes"] += size
        if re.match(r"t/[^/]+/m/l0/", key) and key.endswith(".rseg"):
            kind = "data"
        elif re.match(r"t/[^/]+/m/c/", key):
            kind = "commit"
        elif key.startswith("sys/qualify/"):
            kind = "qualify"
        else:
            kind = "other"
        kinds[kind]["count"] += 1
        kinds[kind]["bytes"] += size
    for field in ("count", "bytes"):
        summed = sum(k[field] for k in kinds.values())
        if summed != total[field]:
            print(f"the kinds' {field} sum to {summed}, the listing's total is {total[field]}")
            sys.exit(4)
    doc = {"bucket": bucket, "endpoint": endpoint, **kinds, "total": total}
    verdict = None
    if band:
        lo, hi = (int(x) for x in band.split(":"))
        n = kinds["data"]["count"]
        verdict = "above" if n > hi else "below" if n < lo else "ok"
        doc["band"] = {"min": lo, "max": hi, "counts": "data", "verdict": verdict}
    else:
        doc["band"] = "not set: no --ravel-objects-band given"
    dump(out_path, doc)
    line = (f"data {kinds['data']['count']} ({kinds['data']['bytes']} B), "
            f"commit {kinds['commit']['count']}, qualify {kinds['qualify']['count']}, "
            f"other {kinds['other']['count']}, total {total['count']} ({total['bytes']} B) in {bucket}")
    if verdict is None:
        print(line + "; no --ravel-objects-band set, counts stamped only")
    elif verdict == "ok":
        print(line + f"; data count inside --ravel-objects-band {band}")
    else:
        print(line + f"; data count {kinds['data']['count']} {verdict} --ravel-objects-band {band}")
        sys.exit(3)
elif op == "newest-time":
    # The newest generated sample, in seconds, as metricsbench_query evaluates
    # an instant entry: anchor_ms + (steps - 1) * scrape_interval_secs * 1000.
    p = load(args[0])["profile"]
    ms = p["run"]["anchor_ms"] + (p["run"]["steps"] - 1) * p["scrape_interval_secs"] * 1000
    print(f"{ms // 1000}.{ms % 1000:03d}")
elif op == "corpus-query":
    corpus, entry = args
    for e in load(corpus).get("entries", []):
        if e.get("id") == entry:
            print(e["promql"])
            sys.exit(0)
    sys.exit(3)
elif op == "corpus-entry-profiles":
    corpus, entry = args
    for e in load(corpus).get("entries", []):
        if e.get("id") == entry:
            print(" ".join(e.get("profiles", [])))
            sys.exit(0)
    sys.exit(3)
elif op == "falsification":
    body_path, code, out_path, entry, query, at, max_samples, profile = args
    doc = {"entry": entry, "query": query, "time": at, "profile": profile,
           "query_max_samples": int(max_samples), "http_status": int(code),
           "status": None, "errorType": None, "error": None, "result_series_count": None}
    try:
        body = load(body_path)
    except (OSError, ValueError) as e:
        doc["error"] = f"response body is not JSON: {type(e).__name__}"
        dump(out_path, doc)
        sys.exit(3)
    doc["status"] = body.get("status")
    doc["errorType"] = body.get("errorType")
    doc["error"] = body.get("error")
    result = (body.get("data") or {}).get("result")
    if isinstance(result, list):
        doc["result_series_count"] = len(result)
    dump(out_path, doc)
else:
    print(f"unknown op {op}", file=sys.stderr)
    sys.exit(2)
PY

py() {
  python3 -I -c "$PYLIB" "$@"
}

stamp_set() { py set "$STAMPS" "$1" "$2"; }
stamp_str() { py setstr "$STAMPS" "$1" "$2"; }

# --- arguments ---------------------------------------------------------------

profile=""
ravel_image=""
ravel_image_id=""
ravel_source_commit=""
bin_dir=""
out_dir=""
anchor_ms=""
passes=""
reps=""
keep=0
diag_max_samples=""
folded_catalog_get_max_fraction="0.05"
folded_catalog_get_floor="64"
real_s3_prefix=""
real_s3_region=""
objects_band=""
bands=()

is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }

while [ $# -gt 0 ]; do
  case "$1" in
    --keep)
      keep=1
      shift
      continue
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --profile | --ravel-image | --ravel-image-id | --ravel-source-commit | --bin-dir | --out-dir | \
      --anchor-ms | --passes | --reps | --rss-band | --diagnostic-prometheus-max-samples | \
      --real-s3-bucket-prefix | --real-s3-region | --ravel-objects-band | \
      --folded-catalog-get-max-fraction | --folded-catalog-get-floor)
      [ $# -ge 2 ] || usage_error "$1 needs a value"
      ;;
    *)
      usage_error "unknown argument: $1"
      ;;
  esac
  case "$1" in
    --profile) profile="$2" ;;
    --ravel-image) ravel_image="$2" ;;
    --ravel-image-id) ravel_image_id="$2" ;;
    --ravel-source-commit) ravel_source_commit="$2" ;;
    --bin-dir) bin_dir="$2" ;;
    --out-dir) out_dir="$2" ;;
    --anchor-ms) anchor_ms="$2" ;;
    --passes) passes="$2" ;;
    --reps) reps="$2" ;;
    --rss-band) bands+=("$2") ;;
    --diagnostic-prometheus-max-samples) diag_max_samples="$2" ;;
    --real-s3-bucket-prefix) real_s3_prefix="$2" ;;
    --real-s3-region) real_s3_region="$2" ;;
    --folded-catalog-get-max-fraction) folded_catalog_get_max_fraction="$2" ;;
    --folded-catalog-get-floor) folded_catalog_get_floor="$2" ;;
    --ravel-objects-band) objects_band="$2" ;;
  esac
  shift 2
done

diagnostic=0
[ -n "$diag_max_samples" ] && diagnostic=1

[ -n "$profile" ] || usage_error "--profile is required"
[ -n "$bin_dir" ] || usage_error "--bin-dir is required"
[ -n "$out_dir" ] || usage_error "--out-dir is required"
real_s3=0
if [ -n "$real_s3_prefix" ] || [ -n "$real_s3_region" ]; then
  real_s3=1
  [ -n "$real_s3_prefix" ] && [ -n "$real_s3_region" ] ||
    usage_error "--real-s3-bucket-prefix and --real-s3-region go together"
  [ "$diagnostic" -eq 0 ] ||
    usage_error "--diagnostic-prometheus-max-samples starts Prometheus alone, which uses no object store; it takes no --real-s3 flags"
  # 44 leaves room for the longest suffix inside S3's 63-character bucket name.
  [[ "$real_s3_prefix" =~ ^[a-z0-9][a-z0-9-]{0,43}$ ]] ||
    usage_error "--real-s3-bucket-prefix takes lowercase letters, digits and hyphens, at most 44 characters, got '$real_s3_prefix'"
  [[ "$real_s3_region" =~ ^[a-z]{2}(-[a-z]+)+-[0-9]+$ ]] ||
    usage_error "--real-s3-region takes an AWS region name, got '$real_s3_region'"
fi
if [ -n "$ravel_image_id" ]; then
  [ -n "$ravel_image" ] ||
    usage_error "--ravel-image-id needs --ravel-image REF naming the local tag it identifies"
  [[ "$ravel_image" != *@sha256:* ]] ||
    usage_error "give either --ravel-image REF@sha256:DIGEST (published) or --ravel-image REF --ravel-image-id sha256:ID (local build), not both"
fi
if [ "$diagnostic" -eq 0 ]; then
  [ -n "$ravel_image" ] ||
    usage_error "a Ravel image is required: --ravel-image REF@sha256:DIGEST (published) or --ravel-image REF --ravel-image-id sha256:ID (local build)"
  [ -n "$ravel_source_commit" ] || usage_error "--ravel-source-commit is required"
fi
for pair in "--anchor-ms:$anchor_ms" "--passes:$passes" "--reps:$reps" \
  "--diagnostic-prometheus-max-samples:$diag_max_samples"; do
  flag="${pair%%:*}"
  value="${pair#*:}"
  [ -z "$value" ] || is_uint "$value" || usage_error "$flag takes a non-negative integer, got '$value'"
done
for band in ${bands[@]+"${bands[@]}"}; do
  [[ "$band" =~ ^[A-Za-z0-9_.-]+:[0-9]+:[0-9]+$ ]] ||
    usage_error "--rss-band takes CONTAINER:MIN_BYTES:MAX_BYTES, got '$band'"
  lo="${band#*:}"
  lo="${lo%%:*}"
  hi="${band##*:}"
  [ "$lo" -le "$hi" ] || usage_error "--rss-band $band has its minimum above its maximum"
done
[[ "$folded_catalog_get_max_fraction" =~ ^(0(\.[0-9]+)?|\.[0-9]+|1(\.0+)?)$ ]] ||
  usage_error "--folded-catalog-get-max-fraction takes a decimal from 0 to 1, got '$folded_catalog_get_max_fraction'"
is_uint "$folded_catalog_get_floor" ||
  usage_error "--folded-catalog-get-floor takes a whole number, got '$folded_catalog_get_floor'"
if [ -n "$objects_band" ]; then
  [ "$diagnostic" -eq 0 ] ||
    usage_error "--diagnostic-prometheus-max-samples starts no Ravel, so there are no Ravel objects to count; it takes no --ravel-objects-band"
  [[ "$objects_band" =~ ^[0-9]+:[0-9]+$ ]] ||
    usage_error "--ravel-objects-band takes MIN:MAX, got '$objects_band'"
  [ "${objects_band%%:*}" -le "${objects_band##*:}" ] ||
    usage_error "--ravel-objects-band $objects_band has its minimum above its maximum"
fi
if [ "$diagnostic" -eq 1 ] && [ "$diag_max_samples" -eq 0 ]; then
  usage_error "--diagnostic-prometheus-max-samples must be at least 1"
fi

command -v python3 >/dev/null 2>&1 || {
  say "python3 is required"
  exit "$EXIT_STEP"
}

# --- refusals ----------------------------------------------------------------

ravel_image_kind=""
if [ -n "$ravel_image_id" ]; then
  ravel_image_kind="local-build"
  [ "$real_s3" -eq 0 ] ||
    refuse "--ravel-image-id with --real-s3-bucket-prefix: the real-S3 lane produces publishable figures, and a local build is never publishable"
  [[ "$ravel_image_id" =~ ^sha256:[0-9a-f]{64}$ ]] ||
    refuse "--ravel-image-id '$ravel_image_id' is not sha256:<64 hex>"
  [ "$profile" = "ci" ] ||
    refuse "--ravel-image-id with --profile $profile: a local build is for the nightly only (--profile ci); a comparable run needs a published --ravel-image REF@sha256:DIGEST"
elif [ -n "$ravel_image" ]; then
  ravel_image_kind="published"
fi
if [ "$ravel_image_kind" = "published" ] && ! [[ "$ravel_image" =~ @sha256:[0-9a-f]{64}$ ]]; then
  refuse "--ravel-image '$ravel_image' carries no @sha256: digest; a tag is a moving reference and the run could not say what it measured (a local build is named by --ravel-image-id sha256:ID instead)"
fi
if [ -n "$ravel_source_commit" ] && ! [[ "$ravel_source_commit" =~ ^[0-9a-f]{40}$ ]]; then
  refuse "--ravel-source-commit '$ravel_source_commit' is not a full 40-character commit id"
fi
for bin in metricsbench_ingest metricsbench_query metricsbench_report; do
  [ -x "${bin_dir}/${bin}" ] || refuse "${bin_dir}/${bin} is missing or not executable"
done
for f in stamps.json report.json falsification.json ingest.json; do
  [ ! -e "${out_dir}/${f}" ] || refuse "${out_dir} already holds ${f} from an earlier run; pass a fresh --out-dir"
done
grid="$(py profile-grid "$WORKLOAD" "$profile")" ||
  refuse "the workload ${WORKLOAD} declares no profile '$profile'"
# Each HTTP row stops once its projected ingest wall time passes this ceiling
# (ADR-0927, 2026-10-28 amendment, decision 7), so a stalled receiver fails
# the run in minutes instead of holding the host for days.
case "$profile" in
  ci) max_ingest_secs=600 ;;
  cardinality) max_ingest_secs=14400 ;;
  history) max_ingest_secs=72000 ;;
  churn) max_ingest_secs=18000 ;;
  *) refuse "no ingest wall-time ceiling is set for profile '$profile'; add one to run.sh" ;;
esac
# Per-request timeout of every read-back query, the same for every engine.
# The read-back's first query lists every series of the profile, which on
# cardinality is 1,000,000 of them; the write timeout does not fit it.
case "$profile" in
  ci) readback_timeout_secs=30 ;;
  cardinality) readback_timeout_secs=900 ;;
  history) readback_timeout_secs=300 ;;
  churn) readback_timeout_secs=900 ;;
  *) refuse "no read-back timeout is set for profile '$profile'; add one to run.sh" ;;
esac
if [ "$diagnostic" -eq 1 ]; then
  # The falsification query only tests --query.max-samples on a profile whose
  # window the entry declares; elsewhere it loads a few samples, cannot reach
  # the limit, and a "success" would falsify nothing.
  entry_profiles="$(py corpus-entry-profiles "$CORPUS" "$FALSIFICATION_ENTRY")" ||
    refuse "the corpus has no entry ${FALSIFICATION_ENTRY}"
  # An empty list means every profile, as promql_corpus::CorpusEntry reads it.
  case " ${entry_profiles} " in
    "  ") ;;
    *" ${profile} "*) ;;
    *) refuse "--diagnostic-prometheus-max-samples sends ${FALSIFICATION_ENTRY}, which the corpus declares for '${entry_profiles}', not '${profile}'; on this profile it cannot reach the limit and falsifies nothing" ;;
  esac
fi
interval_secs="${grid%% *}"
duration_secs="${grid##* }"
interval_ms=$((interval_secs * 1000))
anchor_source="flag"
if [ -n "$anchor_ms" ]; then
  [ $((anchor_ms % interval_ms)) -eq 0 ] ||
    refuse "--anchor-ms $anchor_ms is not on profile '$profile''s ${interval_ms} ms scrape grid (remainder $((anchor_ms % interval_ms)))"
else
  # Ravel drops a sample older than its --max-ingest-lag (45d) and refuses
  # one more than 10m in the future, so the default window ends 10 minutes
  # before now, on the scrape grid. The stream digest then differs per run;
  # pass --anchor-ms to replay a fixed one.
  now_ms=$(($(date +%s) * 1000))
  anchor_ms=$(((now_ms - duration_secs * 1000 - 600000) / interval_ms * interval_ms))
  anchor_source="clock: the profile's window ending at least 10 minutes before the run started, on the scrape grid"
fi

host_arch="$(uname -m)"
if [ "$diagnostic" -eq 0 ] && [ "$real_s3" -eq 0 ]; then
  # The binary runs inside the engine's Linux containers, so the engine's arch
  # picks the asset, not the host's (macOS reports arm64 for linux/aarch64).
  engine_arch="$("$DOCKER" version --format '{{.Server.Arch}}' 2>/dev/null)" || engine_arch=""
  if [ -n "$engine_arch" ]; then
    engine_arch_source="docker version (Server.Arch)"
  else
    engine_arch="$host_arch"
    engine_arch_source="uname -m (the Docker engine did not answer docker version)"
  fi
  case "$engine_arch" in
    amd64 | x86_64) s3s_fs_arch="x86_64" ;;
    arm64 | aarch64) s3s_fs_arch="aarch64" ;;
    *) refuse "no s3s-fs release asset is pinned for engine arch '$engine_arch' (from ${engine_arch_source}); only amd64/x86_64 and arm64/aarch64 are" ;;
  esac
  case "$s3s_fs_arch" in
    x86_64) s3s_fs_sha256="$S3S_FS_SHA256_X86_64" ;;
    aarch64) s3s_fs_sha256="$S3S_FS_SHA256_AARCH64" ;;
  esac
  s3s_fs_asset="s3s-fs-linux-${s3s_fs_arch}"
  # macOS ships shasum, not sha256sum.
  if command -v sha256sum >/dev/null 2>&1; then
    sha256_cmd=(sha256sum)
  elif command -v shasum >/dev/null 2>&1; then
    sha256_cmd=(shasum -a 256)
  else
    refuse "neither sha256sum nor shasum is on PATH; the s3s-fs binary cannot be checked against its pin"
  fi
fi

if [ "$ravel_image_kind" = "local-build" ]; then
  local_id="$("$DOCKER" image inspect "$ravel_image" 2>/dev/null | py image-inspect-local "$ravel_image" "$ravel_image_id")" ||
    refuse "docker image inspect $ravel_image does not report Id $ravel_image_id (got: ${local_id:-no such image}); the tag no longer names the build the run was given"
fi

if [ "$diagnostic" -eq 1 ]; then
  compose_files=(-f "${here}/docker-compose.yml" -f "${out_dir}/prometheus-max-samples.override.yml")
else
  compose_files=(-f "${here}/docker-compose.yml" -f "${here}/ravel.yml")
  if [ "$real_s3" -eq 1 ]; then
    compose_files+=(-f "${here}/real-s3.yml")
    export METRICSBENCH_S3_BUCKET_PREFIX="$real_s3_prefix" METRICSBENCH_S3_REGION="$real_s3_region"
  fi
fi
# compose interpolates ravel.yml on every command, `ps` and `down` included.
export RAVEL_IMAGE="${ravel_image:-unset-diagnostic-run-starts-no-ravel}"
# The same for docker-compose.yml's S3S_FS_BIN, which fetch-s3s-fs replaces
# with the verified binary before `up`; this path is never started.
export S3S_FS_BIN="/nonexistent/s3s-fs-not-fetched"

compose() {
  "$DOCKER" compose "${compose_files[@]}" "$@"
}

leftover_containers="$("$DOCKER" compose -f "${here}/docker-compose.yml" -f "${here}/ravel.yml" ps -a -q 2>/dev/null)"
[ -z "$leftover_containers" ] ||
  refuse "compose project '$PROJECT' already has containers; tear it down with docker compose -f docker-compose.yml -f ravel.yml down -v"
leftover_volumes="$("$DOCKER" volume ls -q --filter "label=com.docker.compose.project=${PROJECT}" 2>/dev/null)"
[ -z "$leftover_volumes" ] ||
  refuse "compose project '$PROJECT' left volumes (${leftover_volumes//$'\n'/ }); an earlier run's data would be measured. Remove them with docker volume rm"

# --- stamps ------------------------------------------------------------------

mkdir -p "$out_dir" || refuse "cannot create --out-dir $out_dir"
out_dir="$(cd "$out_dir" && pwd)"
bin_dir="$(cd "$bin_dir" && pwd)"
STAMPS="${out_dir}/stamps.json"

instance_type() {
  # The IMDS session token and the answer stay outside the out-dir, which a
  # caller may upload as an artifact.
  local token="" code scratch
  scratch="$(mktemp -d)" || { echo "unknown"; return 0; }
  code="$(curl -sS --max-time 2 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    -o "${scratch}/token" -w '%{http_code}' "${IMDS}/latest/api/token" 2>/dev/null)" || code=""
  if [ "$code" = "200" ]; then
    token="$(cat "${scratch}/token")"
  fi
  if [ -n "$token" ]; then
    code="$(curl -sS --max-time 2 -H "X-aws-ec2-metadata-token: ${token}" \
      -o "${scratch}/type" -w '%{http_code}' "${IMDS}/latest/meta-data/instance-type" 2>/dev/null)" || code=""
    if [ "$code" = "200" ] && [ -s "${scratch}/type" ]; then
      cat "${scratch}/type"
      rm -rf "$scratch"
      return 0
    fi
  fi
  rm -rf "$scratch"
  echo "unknown"
}

git_head() {
  git -C "$1" rev-parse HEAD 2>/dev/null || echo "unavailable: $1 is not inside a git checkout"
}

host_instance_type="$(instance_type)"
printf '{}\n' >"$STAMPS" || refuse "cannot write $STAMPS"
stamp_str schema "metricsbench-run-stamps/1"
stamp_str status running
stamp_set diagnostic "$([ "$diagnostic" -eq 1 ] && echo true || echo false)"
stamp_str profile "$profile"
stamp_set anchor_ms "$anchor_ms"
stamp_str anchor_source "$anchor_source"
stamp_str started_at "$(utc_now)"
stamp_set ended_at null
stamp_str ravel_image.kind "${ravel_image_kind:-not given: diagnostic run}"
stamp_str ravel_image.reference "${ravel_image:-not given: diagnostic run}"
if [ "$ravel_image_kind" = "local-build" ]; then
  stamp_str ravel_image.image_id "$ravel_image_id"
fi
stamp_set ravel_image.resolved null
stamp_str ravel_source_commit "${ravel_source_commit:-not given: diagnostic run}"
stamp_str harness_commit "$(git_head "$bin_dir")"
stamp_str run_script_commit "$(git_head "$here")"
stamp_str bin_dir "$bin_dir"
if [ "$real_s3" -eq 1 ]; then
  stamp_str store_kind s3
else
  stamp_str store_kind s3s-fs
fi
stamp_str instance_type "$host_instance_type"
stamp_str host.arch "$host_arch"
stamp_str host.kernel "$(uname -sr)"
stamp_str passes "${passes:-default}"
stamp_str reps_per_pass "${reps:-default}"
stamp_set rss_bands "$(printf '%s\n' ${bands[@]+"${bands[@]}"} | python3 -I -c 'import json,sys; print(json.dumps([l.strip() for l in sys.stdin if l.strip()]))')"
if [ "$diagnostic" -eq 1 ]; then
  stamp_set prometheus_query_max_samples "$diag_max_samples"
fi
stamp_set comparator_images null
stamp_set ravel_catalog_fold null
stamp_set mimir_readiness null
stamp_set ravel_catalog_fold_writers_stopped null
stamp_set ravel_restart null
stamp_set readback_folded null
stamp_set ravel_objects null
stamp_set object_store null
stamp_set in_process_row null
stamp_set steps_completed '[]'
stamp_set failed_after_ingest '[]'
stamp_set deferred_failures '[]'
stamp_set failed_step null
stamp_set exit_code null
stamp_set teardown null

# --- teardown ----------------------------------------------------------------

STACK_UP=0
SAMPLER_PID=""
MEMORY_DONE=0
FAILED_STEP=""
# Set by run_ingest in a full run whose ingest verification failed (a row's
# read-back failed or a row stalled): the run goes on to query as a
# diagnostic and exits 70 naming ingest at the end.
INGEST_FAILED=0
INGEST_FAILURE=""
CONTINUE_AFTER_FAILED_VERIFICATION=0
# Set by memory_step --defer in a full run whose memory step failed: the
# folded steps still run, and the run exits for the memory failure after them.
MEMORY_FAILED=0
MEMORY_FAILURE=""
# The ingest hour (Unix seconds / 3600, the catalog's hour bucket) in which
# the ingest step ended; the fold must have folded through it.
INGEST_END_HOUR=""

sample_once() {
  local ids ts
  ids="$(compose ps -q 2>/dev/null)" || return 0
  [ -n "$ids" ] || return 0
  ts="$(utc_now)"
  # shellcheck disable=SC2086 # one container id per word
  "$DOCKER" stats --no-stream --format '{{json .}}' $ids 2>/dev/null |
    py mem-parse "$ts" >>"${out_dir}/memory.jsonl"
}

sampler_loop() {
  trap - EXIT
  while [ ! -e "${out_dir}/.sampler-stop" ]; do
    sample_once
    sleep "$SAMPLE_INTERVAL"
  done
}

stop_sampler() {
  [ -n "$SAMPLER_PID" ] || return 0
  : >"${out_dir}/.sampler-stop"
  wait "$SAMPLER_PID" 2>/dev/null
  SAMPLER_PID=""
  rm -f "${out_dir}/.sampler-stop"
  sample_once
}

# memory_summary: stop the sampler, inspect every compose container, write
# memory-summary.json and each violation's line to memory-verdict.txt. It runs
# in the main shell, never in $(...), so the sampler state it clears sticks.
# Returns 3 on a band violation or an OOM kill.
memory_summary() {
  local ids code=0
  stop_sampler
  MEMORY_DONE=1
  ids="$(compose ps -a -q 2>/dev/null)"
  if [ -n "$ids" ]; then
    # shellcheck disable=SC2086 # one container id per word
    "$DOCKER" inspect $ids >"${out_dir}/containers-inspect.json" 2>/dev/null ||
      printf '[]\n' >"${out_dir}/containers-inspect.json"
  else
    printf '[]\n' >"${out_dir}/containers-inspect.json"
  fi
  py summary "${out_dir}/memory.jsonl" "${out_dir}/containers-inspect.json" \
    "${out_dir}/memory-summary.json" "$SAMPLE_INTERVAL" ${bands[@]+"${bands[@]}"} \
    >"${out_dir}/memory-verdict.txt" || code=$?
  return "$code"
}

on_exit() {
  local code=$? down_code=0
  trap - EXIT
  if [ "$STACK_UP" -eq 1 ]; then
    if [ "$MEMORY_DONE" -eq 0 ] && [ -n "$SAMPLER_PID" ]; then
      # Best effort on a failed run: the summary is evidence, the exit code
      # stays the failed step's.
      memory_summary >/dev/null 2>&1
    fi
    stop_sampler
    compose logs --no-color >"${out_dir}/compose.log" 2>&1
    if [ "$keep" -eq 1 ]; then
      stamp_str teardown "kept (--keep)"
      say "--keep: the stack is still up"
    else
      compose down -v >"${out_dir}/compose-down.log" 2>&1 || down_code=$?
      if [ "$down_code" -eq 0 ]; then
        stamp_str teardown "docker compose down -v"
      else
        stamp_str teardown "docker compose down -v failed (exit ${down_code}), see compose-down.log"
        say "teardown failed (exit ${down_code}); see ${out_dir}/compose-down.log"
        if [ "$code" -eq 0 ]; then
          FAILED_STEP="teardown"
          code="$EXIT_STEP"
        fi
      fi
    fi
  else
    stamp_str teardown "nothing to tear down"
  fi
  [ -z "$FAILED_STEP" ] || stamp_str failed_step "$FAILED_STEP"
  stamp_set exit_code "$code"
  stamp_str ended_at "$(utc_now)"
  if [ "$code" -eq 0 ]; then
    stamp_str status passed
  else
    stamp_str status failed
  fi
  exit "$code"
}
trap on_exit EXIT

fail_step() {
  local also=""
  [ "$MEMORY_FAILED" -eq 0 ] || also="; step memory had failed before it: ${MEMORY_FAILURE}"
  if [ "$INGEST_FAILED" -eq 1 ]; then
    say "step $1 failed: ${*:2}${also}"
    py append "$STAMPS" failed_after_ingest "$1"
    fail_after_ingest
  fi
  FAILED_STEP="$1"
  shift
  say "step $FAILED_STEP failed: $*${also}"
  exit "$EXIT_STEP"
}

# fail_after_ingest: end a run whose ingest verification failed. The exit
# code and failed_step name ingest whatever later step failed or passed;
# failed_after_ingest lists the later ones that failed.
fail_after_ingest() {
  FAILED_STEP="ingest"
  if [ -s "${out_dir}/report.json" ]; then
    say "step ingest failed: ${INGEST_FAILURE}; the run went on as a diagnostic, report.json is not a result"
  else
    say "step ingest failed: ${INGEST_FAILURE}; the run went on as a diagnostic and wrote no report.json"
  fi
  exit "$EXIT_STEP"
}

step_done() {
  py append "$STAMPS" steps_completed "$1"
  say "step $1 done"
}

# show_log FILE: the last lines of a step's log on stderr, so the failure is
# readable without opening the out-dir.
show_log() {
  [ -s "$1" ] || return 0
  echo "--- last lines of $1 ---" >&2
  tail -n 20 "$1" >&2
}

# --- steps -------------------------------------------------------------------

wait_ready() {
  local name="$1" url="$2" step="${3:-ready}" i code
  for ((i = 1; i <= READY_TRIES; i++)); do
    code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)" || code=""
    if [ "$code" = "200" ]; then
      return 0
    fi
    sleep "$READY_INTERVAL"
  done
  fail_step "$step" "$name did not answer 200 on $url after $READY_TRIES tries ${READY_INTERVAL}s apart (last status '${code}')"
}

check_one_shot() {
  local service="$1" id state
  id="$(compose ps -a -q "$service" 2>/dev/null)"
  [ -n "$id" ] || fail_step qualify "no container for the one-shot $service"
  state="$("$DOCKER" inspect "$id" 2>/dev/null | py one-shot-exit)" ||
    fail_step qualify "docker inspect of $service failed"
  [ "$state" = "exited 0" ] || fail_step qualify "$service did not exit 0 (state: $state)"
}

start_sampler() {
  rm -f "${out_dir}/.sampler-stop"
  : >>"${out_dir}/memory.jsonl"
  sampler_loop &
  SAMPLER_PID=$!
  step_done sampler
}

run_ingest() {
  local code=0
  "${bin_dir}/metricsbench_ingest" "$@" >"${out_dir}/ingest.json" 2>"${out_dir}/ingest.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/ingest.log"
    local failed=""
    if [ "$CONTINUE_AFTER_FAILED_VERIFICATION" -eq 1 ] &&
      failed="$(py ingest-verification-failed "${out_dir}/ingest.json")"; then
      INGEST_FAILED=1
      INGEST_FAILURE="metricsbench_ingest exited $code: ${failed}"
      FAILED_STEP="ingest"
      stamp_str failed_step ingest
      say "step ingest failed: ${INGEST_FAILURE}; going on to query as a diagnostic"
      return 0
    fi
    fail_step ingest "metricsbench_ingest exited $code; see ${out_dir}/ingest.log"
  fi
  step_done ingest
}

# memory_step [--defer]: with --defer a failure is listed in deferred_failures
# and the run goes on; end_deferred_memory exits for it later.
memory_step() {
  local code=0 verdict
  memory_summary || code=$?
  if [ "$code" -ne 0 ]; then
    verdict="$(cat "${out_dir}/memory-verdict.txt" 2>/dev/null)"
    verdict="${verdict//$'\n'/; }"
    if [ "${1:-}" = "--defer" ]; then
      say "step memory failed: ${verdict}; going on to the folded steps first"
      py append "$STAMPS" deferred_failures memory
      MEMORY_FAILED=1
      MEMORY_FAILURE="$verdict"
      return 0
    fi
    say "step memory failed: ${verdict}"
    if [ "$INGEST_FAILED" -eq 1 ]; then
      py append "$STAMPS" failed_after_ingest memory
      fail_after_ingest
    fi
    FAILED_STEP="memory"
    exit "$EXIT_MEMORY"
  fi
  step_done memory
}

# end_deferred_memory: after the folded steps, end a run whose memory step
# failed the way memory_step would have.
end_deferred_memory() {
  [ "$MEMORY_FAILED" -eq 1 ] || return 0
  say "step memory failed: ${MEMORY_FAILURE}; the folded steps ran after it"
  if [ "$INGEST_FAILED" -eq 1 ]; then
    py append "$STAMPS" failed_after_ingest memory
    fail_after_ingest
  fi
  FAILED_STEP="memory"
  exit "$EXIT_MEMORY"
}

# fold_field KEY: KEY's value in <out>/fold.out, which must carry it exactly
# once.
fold_field() {
  local n
  n="$(grep -c "^[[:space:]]*$1[[:space:]]*:" "${out_dir}/fold.out" 2>/dev/null)"
  [ "$n" = 1 ] || { say "fold.out carries $1 ${n:-0} times, not once"; return 1; }
  sed -n "s/^[[:space:]]*$1[[:space:]]*:[[:space:]]*//p" "${out_dir}/fold.out"
}

fold_step() {
  local code=0 seal watermark entries buckets no_op precondition="met"
  compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog fold \
    --tenant "$RAVEL_TENANT" --shards "$RAVEL_SHARDS" --signal metrics --writers-stopped \
    >"${out_dir}/fold.out" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/fold.out"
    fail_step fold "ravel-cli catalog fold exited $code; see ${out_dir}/fold.out"
  fi
  seal="$(fold_field seal_through_hour)" || fail_step fold "no single seal_through_hour in fold.out"
  watermark="$(fold_field watermark_hour)" || fail_step fold "no single watermark_hour in fold.out"
  entries="$(fold_field entry_count)" || fail_step fold "no single entry_count in fold.out"
  buckets="$(fold_field buckets_folded)" || fail_step fold "no single buckets_folded in fold.out"
  no_op="$(fold_field no_op)" || no_op=""
  if ! [[ "$seal" =~ ^Some\(([0-9]+)\)$ ]]; then
    precondition="fold sealed nothing: seal_through_hour=$seal"
  elif ! [[ "$watermark" =~ ^Some\(([0-9]+)\)$ ]] || [ "${BASH_REMATCH[1]}" -lt "$INGEST_END_HOUR" ]; then
    precondition="watermark_hour=$watermark is before hour $INGEST_END_HOUR, in which the ingest step ended"
  elif ! [[ "$entries" =~ ^[0-9]+$ ]] || ! [[ "$buckets" =~ ^[0-9]+$ ]]; then
    precondition="entry_count '$entries' or buckets_folded '$buckets' is not a whole number"
  fi
  stamp_set ravel_catalog_fold_writers_stopped \
    "$(py fold-stamp "$seal" "$watermark" "$entries" "$buckets" "$no_op" "$INGEST_END_HOUR" "$precondition")"
  [ "$precondition" = "met" ] || fail_step fold "$precondition"
  step_done fold
}

# stop_step: the --writers-stopped fold is safe only once the writer has
# exited (ADR-2677 decision 1), so ravel-server is down for the whole fold.
stop_step() {
  local code=0
  compose stop ravel-server >"${out_dir}/ravel-stop.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/ravel-stop.log"
    stamp_str ravel_restart "not stopped: docker compose stop ravel-server exited $code; no fold ran"
    fail_step stop "docker compose stop ravel-server exited $code"
  fi
  stamp_str ravel_restart "stopped: docker compose stop ravel-server before the fold; not started again"
  step_done stop
}

start_step() {
  local code=0 stopped="stopped: docker compose stop ravel-server before the fold"
  compose start ravel-server >"${out_dir}/ravel-start.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/ravel-start.log"
    stamp_str ravel_restart "${stopped}; docker compose start ravel-server after it exited $code"
    fail_step start "docker compose start ravel-server exited $code"
  fi
  wait_ready ravel "${RAVEL_BASE}/api/v1/status/buildinfo" start
  stamp_str ravel_restart "${stopped}; started: docker compose start ravel-server after it, then ravel answered 200 on /api/v1/status/buildinfo"
  step_done start
}

# scrape_ravel_metrics FILE: Ravel's /metrics into FILE; non-zero unless it
# answered 200 with a body.
scrape_ravel_metrics() {
  local code=0 http
  http="$(curl -sS --max-time 10 -o "$1" -w '%{http_code}' "${RAVEL_BASE}/metrics" 2>/dev/null)" || code=$?
  [ "$code" -eq 0 ] && [ "$http" = "200" ] && [ -s "$1" ]
}

readback_folded_step() {
  local code=0 stamp_code=0 stamp before="${out_dir}/ravel-metrics-before-folded-readback.txt"
  local after="${out_dir}/ravel-metrics-after-folded-readback.txt" before_arg after_arg missing="" verdict
  before_arg="$before"
  scrape_ravel_metrics "$before" || { before_arg=""; missing="before"; }
  "${bin_dir}/metricsbench_ingest" --readback-only --ingest-report "${out_dir}/ingest.json" \
    --readback-system ravel --workload "$WORKLOAD" --profile "$profile" --anchor-ms "$anchor_ms" \
    --ravel-query-url "$RAVEL_BASE" --ravel-bearer-token-env "$RAVEL_TOKEN_ENV" \
    --readback-timeout-secs "$readback_timeout_secs" --readback-settle-secs "$READBACK_SETTLE_SECS" \
    >"${out_dir}/readback-folded.json" 2>"${out_dir}/readback-folded.log" || code=$?
  after_arg="$after"
  scrape_ravel_metrics "$after" || { after_arg=""; missing="${missing:+$missing and }after"; }
  stamp="$(py readback-folded "${out_dir}/readback-folded.json" "$code" "$before_arg" "$after_arg" \
    "${out_dir}/ravel-metrics-at-query.txt" "${out_dir}/ravel-objects.json" "$folded_catalog_get_max_fraction" \
    "$folded_catalog_get_floor")" || stamp_code=$?
  [ -z "$stamp" ] || stamp_set readback_folded "$stamp"
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/readback-folded.log"
    fail_step readback-folded "metricsbench_ingest --readback-only exited $code: $(py get "$STAMPS" readback_folded.error 2>/dev/null)"
  fi
  [ "$stamp_code" -eq 0 ] ||
    fail_step readback-folded "metricsbench_ingest --readback-only exited 0 without a read-back report"
  [ "$(py get "$STAMPS" readback_folded.matches_expected)" = "True" ] ||
    fail_step readback-folded "metricsbench_ingest --readback-only exited 0 without matches_expected true"
  [ -z "$missing" ] || fail_step readback-folded "the ${missing} scrape of ${RAVEL_BASE}/metrics failed"
  verdict="$(py get "$STAMPS" readback_folded.catalog_get_verdict 2>/dev/null)" || verdict=""
  [ "$verdict" = "within" ] || [[ "$verdict" == "not discriminating:"* ]] ||
    fail_step readback-folded "folded read-back catalog GETs: ${verdict:-no verdict stamped}"
  step_done readback-folded
}

# folded_readback: stop, fold, start and readback-folded, or their stamps
# saying why they were skipped when the Ravel row did not replay. A failed
# fold ends the run with ravel-server still stopped; teardown removes the
# stack either way, and nothing after the fold would read it.
folded_readback() {
  local why k
  if ! why="$(py ravel-row-state "${out_dir}/ingest.json")"; then
    for k in ravel_catalog_fold_writers_stopped ravel_restart readback_folded; do
      stamp_str "$k" "skipped: $why"
    done
    say "steps stop, fold, start and readback-folded skipped: $why"
    return 0
  fi
  stop_step
  fold_step
  start_step
  readback_folded_step
}

# sha256_of FILE: the file's sha256 as 64 lowercase hex characters.
sha256_of() {
  local out
  out="$("${sha256_cmd[@]}" "$1")" || return 1
  printf '%s\n' "${out%% *}"
}

# fetch_s3s_fs: S3S_FS_BIN for the s3s service. A cached binary is used only
# if its sha256 is the pinned one; otherwise none is cached, and the release
# asset is downloaded beside the cache entry, compared, and moved into it
# only on a match. A mismatch on either path exits 65 and starts nothing.
fetch_s3s_fs() {
  local dir="${S3S_FS_CACHE}/${S3S_FS_RELEASE}" bin got from tmp code=0
  local url="${S3S_FS_URL_BASE}/${s3s_fs_asset}"
  bin="${dir}/${s3s_fs_asset}"
  if [ -e "$bin" ]; then
    got="$(sha256_of "$bin")" || fail_step fetch-s3s-fs "cannot hash the cached $bin"
    if [ "$got" != "$s3s_fs_sha256" ]; then
      FAILED_STEP="fetch-s3s-fs"
      say "refused: the cached $bin has sha256 $got, not the pinned $s3s_fs_sha256 for $S3S_FS_RELEASE; delete it to download again"
      exit "$EXIT_REFUSED"
    fi
    from="cache"
  else
    mkdir -p "$dir" || fail_step fetch-s3s-fs "cannot create the cache directory $dir"
    tmp="${bin}.download.$$"
    curl -fsSL --max-time 600 -o "$tmp" "$url" >"${out_dir}/s3s-fs-fetch.log" 2>&1 || code=$?
    if [ "$code" -ne 0 ]; then
      rm -f "$tmp"
      show_log "${out_dir}/s3s-fs-fetch.log"
      fail_step fetch-s3s-fs "downloading $url failed (curl exit $code)"
    fi
    got="$(sha256_of "$tmp")" || { rm -f "$tmp"; fail_step fetch-s3s-fs "cannot hash the download of $url"; }
    if [ "$got" != "$s3s_fs_sha256" ]; then
      rm -f "$tmp"
      FAILED_STEP="fetch-s3s-fs"
      say "refused: $url has sha256 $got, not the pinned $s3s_fs_sha256; nothing was cached"
      exit "$EXIT_REFUSED"
    fi
    chmod 0755 "$tmp" && mv -f "$tmp" "$bin" ||
      { rm -f "$tmp"; fail_step fetch-s3s-fs "cannot move the verified download into $bin"; }
    from="download"
  fi
  export S3S_FS_BIN="$bin"
  stamp_set object_store "$(python3 -I -c 'import json,sys; print(json.dumps(dict(zip(["implementation","release","arch","engine_arch","engine_arch_source","sha256","url","path","source"], sys.argv[1:]))))' \
    s3s-fs "$S3S_FS_RELEASE" "$s3s_fs_arch" "$engine_arch" "$engine_arch_source" "$got" "$url" "$bin" "$from")"
  step_done fetch-s3s-fs
}

# count_ravel_objects: list Ravel's bucket through the createbuckets service,
# so each lane uses its own endpoint and credentials, and split the keys by
# kind into ravel-objects.json. The aws CLI follows every continuation token
# itself unless told not to.
count_ravel_objects() {
  local bucket endpoint code=0 line
  local list_args=(s3api list-objects-v2 --output json)
  if [ "$real_s3" -eq 1 ]; then
    bucket="${real_s3_prefix}-ravel"
    endpoint="regional AWS endpoint"
  else
    bucket="ravel-metricsbench"
    endpoint="http://s3s:9000"
    list_args=(--endpoint-url "$endpoint" "${list_args[@]}")
  fi
  compose run --rm --no-deps -T --entrypoint aws createbuckets "${list_args[@]}" --bucket "$bucket" \
    >"${out_dir}/ravel-objects-listing.json" 2>"${out_dir}/objects.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/objects.log"
    fail_step objects "listing bucket $bucket exited $code; see ${out_dir}/objects.log"
  fi
  code=0
  line="$(py ravel-objects "${out_dir}/ravel-objects-listing.json" "${out_dir}/ravel-objects.json" \
    "$bucket" "$endpoint" "$objects_band")" || code=$?
  if [ "$code" -eq 0 ] || [ "$code" -eq 3 ]; then
    stamp_set ravel_objects "$(cat "${out_dir}/ravel-objects.json")"
  fi
  [ "$code" -eq 0 ] || fail_step objects "$line"
  say "objects: $line"
  step_done objects
}

run_diagnostic() {
  local code=0 query at http
  compose_config="$("$DOCKER" compose -f "${here}/docker-compose.yml" config --format json 2>"${out_dir}/override.log")" ||
    fail_step override "docker compose config failed; see ${out_dir}/override.log"
  printf '%s' "$compose_config" | py override "$diag_max_samples" \
    >"${out_dir}/prometheus-max-samples.override.yml" 2>>"${out_dir}/override.log" ||
    fail_step override "could not write the Prometheus override; see ${out_dir}/override.log"
  step_done override

  STACK_UP=1
  compose up -d prometheus >"${out_dir}/compose-up.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/compose-up.log"
    fail_step up "docker compose up -d prometheus exited $code"
  fi
  step_done up
  local pid
  pid="$(compose ps -a -q prometheus 2>/dev/null)"
  if [ -n "$pid" ]; then
    stamp_set comparator_images "{\"prometheus\": $("$DOCKER" inspect "$pid" 2>/dev/null |
      python3 -I -c 'import json,sys; r=json.load(sys.stdin); print(json.dumps({"container_image": (r[0].get("Config") or {}).get("Image"), "image_id": r[0].get("Image")}))' 2>/dev/null || echo null)}"
  fi
  stamp_str object_store "not applicable: diagnostic run starts no object store"
  stamp_str ravel_catalog_fold "not applicable: diagnostic run starts no Ravel"
  stamp_str mimir_readiness "not applicable: diagnostic run starts no Mimir"
  stamp_str ravel_catalog_fold_writers_stopped "not applicable: diagnostic run starts no Ravel"
  stamp_str ravel_restart "not applicable: diagnostic run starts no Ravel"
  stamp_str readback_folded "not applicable: diagnostic run starts no Ravel"
  stamp_str ravel_objects "not applicable: diagnostic run starts no Ravel"

  wait_ready prometheus "${PROM_BASE}/-/ready"
  step_done ready
  start_sampler

  # Prometheus is the only row this run needs; an in-process row of the
  # history profile would not fit in memory.
  stamp_str in_process_row skipped
  run_ingest --workload "$WORKLOAD" --profile "$profile" --anchor-ms "$anchor_ms" --no-in-process-row \
    --max-ingest-secs "$max_ingest_secs" --readback-timeout-secs "$readback_timeout_secs" \
    --prometheus-endpoint "${PROM_BASE}/api/v1/write" --prometheus-query-url "$PROM_BASE"

  query="$(py corpus-query "$CORPUS" "$FALSIFICATION_ENTRY")" ||
    fail_step falsification "the corpus has no entry $FALSIFICATION_ENTRY"
  at="$(py newest-time "${out_dir}/ingest.json")" ||
    fail_step falsification "cannot read the newest sample time from ingest.json"
  code=0
  http="$(curl -sS --max-time 300 -o "${out_dir}/falsification-body.json" -w '%{http_code}' \
    --data-urlencode "query=${query}" --data-urlencode "time=${at}" \
    "${PROM_BASE}/api/v1/query" 2>"${out_dir}/falsification.log")" || code=$?
  if [ "$code" -ne 0 ] || [ -z "$http" ] || [ "$http" = "000" ]; then
    fail_step falsification "the instant query to Prometheus failed in transport (curl exit $code)"
  fi
  py falsification "${out_dir}/falsification-body.json" "$http" "${out_dir}/falsification.json" \
    "$FALSIFICATION_ENTRY" "$query" "$at" "$diag_max_samples" "$profile" ||
    fail_step falsification "Prometheus answered HTTP $http with a body that is not JSON"
  step_done falsification

  memory_step
}

run_full() {
  local code=0 resolved line fold
  if [ "$real_s3" -eq 1 ]; then
    stamp_set object_store "$(python3 -I -c 'import json,sys; print(json.dumps({"implementation":"s3","region":sys.argv[1],"credentials":"instance-role","buckets":[sys.argv[2]+"-"+b for b in ("ravel","mimir-blocks","mimir-ruler","mimir-alertmanager")]}))' \
      "$real_s3_region" "$real_s3_prefix")"
  else
    fetch_s3s_fs
  fi

  STACK_UP=1
  compose up -d >"${out_dir}/compose-up.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/compose-up.log"
    fail_step up "docker compose up -d exited $code"
  fi
  step_done up

  check_one_shot createbuckets
  check_one_shot ravel-qualify
  step_done qualify

  code=0
  if [ "$ravel_image_kind" = "local-build" ]; then
    resolved="$("$DOCKER" image inspect "$ravel_image" 2>/dev/null |
      py image-inspect-local "$ravel_image" "$ravel_image_id")" || code=$?
    [ -n "$resolved" ] && stamp_set ravel_image.resolved "$resolved"
    [ "$code" -eq 0 ] ||
      fail_step image "docker image inspect of $ravel_image no longer reports Id $ravel_image_id"
  else
    resolved="$("$DOCKER" image inspect "$ravel_image" 2>/dev/null | py image-inspect "$ravel_image")" || code=$?
    [ -n "$resolved" ] && stamp_set ravel_image.resolved "$resolved"
    [ "$code" -eq 0 ] ||
      fail_step image "docker image inspect of $ravel_image did not list the digest the reference names"
  fi
  step_done image

  wait_ready prometheus "${PROM_BASE}/-/ready"
  wait_ready victoriametrics "${VM_BASE}/health"
  wait_ready mimir "${MIMIR_BASE}/ready"
  wait_ready ravel "${RAVEL_BASE}/api/v1/status/buildinfo"
  step_done ready

  start_sampler

  export "${RAVEL_TOKEN_ENV}=${RAVEL_TOKEN_VALUE}"
  # The in-process diagnostic row holds the whole stream in a memory store.
  # That fits ci's 120,000 samples; cardinality and history do not fit beside
  # four engine containers, and the stack's s3s-fs is the engines' own store,
  # so every other profile runs on its portable rows alone. Those runs lose
  # one check: the portable rows' scalar digests are no longer compared with
  # the in-process row's; they are still compared with each other.
  # --tenant is passed either way so --ravel-tenant always has its match.
  if [ "$profile" = "ci" ]; then
    in_process=(--store memory --tenant "$RAVEL_TENANT")
    stamp_str in_process_row present
  else
    in_process=(--no-in-process-row --tenant "$RAVEL_TENANT")
    stamp_str in_process_row skipped
  fi
  CONTINUE_AFTER_FAILED_VERIFICATION=1
  run_ingest --workload "$WORKLOAD" --profile "$profile" --anchor-ms "$anchor_ms" "${in_process[@]}" \
    --max-ingest-secs "$max_ingest_secs" --readback-timeout-secs "$readback_timeout_secs" \
    --ravel-endpoint "${RAVEL_BASE}/api/v1/write" --ravel-tenant "$RAVEL_TENANT" \
    --ravel-query-url "$RAVEL_BASE" --ravel-bearer-token-env "$RAVEL_TOKEN_ENV" \
    --prometheus-endpoint "${PROM_BASE}/api/v1/write" --prometheus-query-url "$PROM_BASE" \
    --victoriametrics-endpoint "${VM_BASE}/api/v1/write" --victoriametrics-query-url "${VM_BASE}/prometheus" \
    --mimir-endpoint "${MIMIR_BASE}/api/v1/push" --mimir-query-url "${MIMIR_BASE}/prometheus"
  INGEST_END_HOUR=$(($(date -u +%s) / 3600))

  code=0
  line="$(py mimir-readiness "${out_dir}/ingest.json")" || code=$?
  if [ "$code" -ne 0 ] && [ "$INGEST_FAILED" -eq 1 ]; then
    # Already part of the ingest failure; the diagnostic queries regardless.
    stamp_str mimir_readiness "$line"
    say "mimir-readiness: $line"
  else
    [ "$code" -eq 0 ] || fail_step mimir-readiness "$line"
    stamp_str mimir_readiness "$line"
    step_done mimir-readiness
  fi

  count_ravel_objects

  code=0
  "$CAPTURE" --out "${out_dir}/effective-config.json" >"${out_dir}/capture.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/capture.log"
    fail_step capture "capture-effective-config.sh exited $code (1 is a partial capture: a comparator's version is unavailable)"
  fi
  stamp_set comparator_images "$(py capture-images "${out_dir}/effective-config.json")" ||
    fail_step capture "effective-config.json is not readable"
  step_done capture

  local pins=() pin
  while IFS= read -r pin; do
    [ -n "$pin" ] && pins+=(--comparator "$pin")
  done < <(py capture-pins "${out_dir}/effective-config.json" 2>"${out_dir}/provenance.log")
  [ "${#pins[@]}" -eq 6 ] ||
    fail_step provenance "$(cat "${out_dir}/provenance.log")"
  local store_args
  if [ "$real_s3" -eq 1 ]; then
    store_args=(--store-backend s3 --region "$real_s3_region" --endpoint n/a --bills-requests
      --config "store_kind=s3"
      --config "store_bucket_prefix=${real_s3_prefix}")
  else
    store_args=(--store-backend s3s-fs --region us-east-1 --endpoint http://s3s:9000
      --config "store_kind=s3s-fs"
      --config "store_release=${S3S_FS_RELEASE}"
      --config "store_sha256=${s3s_fs_sha256}")
  fi
  local it_env=(env -u RAVEL_INSTANCE_TYPE)
  [ "$host_instance_type" = "unknown" ] || it_env=(env "RAVEL_INSTANCE_TYPE=${host_instance_type}")
  # ravel_git_commit names the source of the image under test, not this
  # checkout: GITHUB_SHA wins over git in metricsbench_report.
  code=0
  "${it_env[@]}" GITHUB_SHA="$ravel_source_commit" "${bin_dir}/metricsbench_report" provenance \
    --workload "$WORKLOAD" --corpus "$CORPUS" \
    "${store_args[@]}" \
    "${pins[@]}" \
    --config "ravel_image=${ravel_image}" \
    --config "ravel_image_kind=${ravel_image_kind}" \
    ${ravel_image_id:+--config "ravel_image_id=${ravel_image_id}"} \
    --config "ravel_source_commit=${ravel_source_commit}" \
    --config "harness_commit=$(py get "$STAMPS" harness_commit)" \
    >"${out_dir}/provenance.json" 2>>"${out_dir}/provenance.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/provenance.log"
    fail_step provenance "metricsbench_report provenance exited $code"
  fi
  step_done provenance

  code=0
  http="$(curl -sS --max-time 10 -o "${out_dir}/ravel-metrics-at-query.txt" -w '%{http_code}' \
    "${RAVEL_BASE}/metrics" 2>/dev/null)" || code=$?
  fold=""
  if [ "$code" -eq 0 ] && [ "$http" = "200" ]; then
    fold="$(py fold-gauge "${out_dir}/ravel-metrics-at-query.txt")" || fold=""
  fi
  stamp_str ravel_catalog_fold "${fold:-absent}"
  step_done fold-state

  local query_args=(--ingest-report "${out_dir}/ingest.json" --workload "$WORKLOAD" --corpus "$CORPUS"
    --provenance "${out_dir}/provenance.json"
    --engine "name=prometheus,url=${PROM_BASE}"
    --engine "name=ravel,url=${RAVEL_BASE},bearer_env=${RAVEL_TOKEN_ENV}"
    --engine "name=victoriametrics,url=${VM_BASE}/prometheus"
    --engine "name=mimir,url=${MIMIR_BASE}/prometheus"
    --diagnostics "${out_dir}/diagnostics.json"
    --out "${out_dir}/report.json")
  [ -z "$passes" ] || query_args+=(--passes "$passes")
  [ -z "$reps" ] || query_args+=(--reps-per-pass "$reps")
  [ "$INGEST_FAILED" -eq 0 ] || query_args+=(--diagnostic-after-failed-ingest)
  code=0
  "${bin_dir}/metricsbench_query" "${query_args[@]}" >"${out_dir}/query.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/query.log"
    fail_step query "metricsbench_query exited $code"
  fi
  [ -s "${out_dir}/report.json" ] || fail_step query "metricsbench_query exited 0 and wrote no report.json"
  step_done query

  code=0
  "${bin_dir}/metricsbench_report" validate "${out_dir}/report.json" >"${out_dir}/validate.log" 2>&1 || code=$?
  if [ "$INGEST_FAILED" -eq 1 ]; then
    # The diagnostic report says run_verdict: failed, which validate refuses.
    if [ "$code" -eq 0 ]; then
      fail_step validate "metricsbench_report validate accepted a report written after a failed ingest"
    fi
    grep -qF "diagnostic of a failed run" "${out_dir}/validate.log" || {
      show_log "${out_dir}/validate.log"
      fail_step validate "metricsbench_report validate exited $code without naming the failed run"
    }
    say "step validate refused the diagnostic report, as it must"
  elif [ "$code" -ne 0 ]; then
    show_log "${out_dir}/validate.log"
    fail_step validate "metricsbench_report validate exited $code"
  fi
  step_done validate

  memory_step --defer
  folded_readback
  end_deferred_memory
  [ "$INGEST_FAILED" -eq 0 ] || fail_after_ingest
}

if [ "$diagnostic" -eq 1 ]; then
  run_diagnostic
else
  run_full
fi
say "done: ${out_dir}"
exit 0
