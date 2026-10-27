#!/usr/bin/env bash
# Run one MetricsBench profile end to end against this directory's compose
# stack (the comparators plus the ravel.yml overlay), stamp what ran, and fail
# loudly (ADR-0927, epic #2821).
#
# Usage:
#   run.sh --profile NAME --ravel-image REF@sha256:DIGEST
#     --ravel-source-commit SHA --bin-dir DIR --out-dir DIR
#     [--anchor-ms N] [--passes N] [--reps N] [--keep]
#     [--rss-band CONTAINER:MIN_BYTES:MAX_BYTES]...
#     [--diagnostic-prometheus-max-samples N]
#
# Steps, in order; the first failure stops the run:
#
#   (refusals)  a --ravel-image without an @sha256: digest, a bin missing from
#               --bin-dir, an out-dir that already holds a run's output, a
#               profile the workload does not declare, an off-grid anchor, or
#               a metricsbench compose project (containers or volumes) left
#               from an earlier run; nothing is started
#   stamps      <out>/stamps.json, written before the first step and completed
#               at the end, whatever the outcome
#   up          docker compose up -d of docker-compose.yml plus ravel.yml,
#               RAVEL_IMAGE set
#   qualify     the one-shots createbuckets and ravel-qualify exited 0
#   image       the Ravel image's resolved digest (docker image inspect),
#               which must include the digest the reference names
#   ready       every engine answers its readiness URL
#   sampler     a background loop appends each compose container's memory use
#               (docker stats --no-stream) to <out>/memory.jsonl, one sample
#               every SAMPLE_INTERVAL seconds after the previous one ends
#   ingest      metricsbench_ingest into all four portable endpoints, read back
#               through their query URLs, into <out>/ingest.json; Ravel's
#               bearer token goes through --ravel-bearer-token-env
#   mimir-readiness  the ingest row for mimir read back what it accepted
#   capture     capture-effective-config.sh into <out>/effective-config.json
#   provenance  metricsbench_report provenance into <out>/provenance.json
#   fold-state  ravel_catalog_fold_last_success_timestamp_seconds from Ravel's
#               /metrics, stamped
#   query       metricsbench_query against all four engines into
#               <out>/report.json
#   validate    metricsbench_report validate <out>/report.json
#   memory      the sampler stopped, <out>/memory-summary.json written (peak
#               and final bytes per container, OOMKilled from docker inspect),
#               every --rss-band checked
#
# With --diagnostic-prometheus-max-samples N the run is the falsification run
# pre-registered on #2821 instead: steps override (a compose override in the
# out-dir setting Prometheus' --query.max-samples=N), up (Prometheus alone),
# ready, sampler, ingest (Prometheus only), falsification (mb_long_range_7d_avg
# once as an instant query at the newest sample, into
# <out>/falsification.json) and memory. It writes no report.json, and
# stamps.json says diagnostic: true. --ravel-image and --ravel-source-commit
# are optional there, since no Ravel is started.
#
# The stack is always torn down with `docker compose ... down -v` (after its
# logs are saved to <out>/compose.log) unless --keep is given.
#
# Exit codes:
#   0   every step passed
#   64  usage error
#   65  a refused input (see refusals above); nothing was started
#   70  a step failed; its name is in the message and in stamps.json
#       (failed_step)
#   75  a --rss-band was violated or a container was OOM-killed
#
# Environment, for tests: METRICSBENCH_DOCKER (default docker),
# METRICSBENCH_CAPTURE (default capture-effective-config.sh beside this
# script), METRICSBENCH_SAMPLE_INTERVAL (default 5), METRICSBENCH_READY_TRIES
# and METRICSBENCH_READY_INTERVAL (default 300 tries 2 s apart),
# METRICSBENCH_IMDS (default http://169.254.169.254).
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

WORKLOAD="${repo_root}/benchmarks/metrics/workload.json"
CORPUS="${repo_root}/benchmarks/metrics/metrics.corpus.json"

# The fixed local-development token ravel.yml's --tenant-token sets. It
# reaches the bins through this variable's name, never through argv.
RAVEL_TOKEN_ENV="METRICSBENCH_RAVEL_TOKEN"
RAVEL_TOKEN_VALUE="metricsbench-token"
RAVEL_TENANT="metricsbench"

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
  sed -n '7,11p' "$0" >&2
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
ravel_source_commit=""
bin_dir=""
out_dir=""
anchor_ms=""
passes=""
reps=""
keep=0
diag_max_samples=""
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
    --profile | --ravel-image | --ravel-source-commit | --bin-dir | --out-dir | \
      --anchor-ms | --passes | --reps | --rss-band | --diagnostic-prometheus-max-samples)
      [ $# -ge 2 ] || usage_error "$1 needs a value"
      ;;
    *)
      usage_error "unknown argument: $1"
      ;;
  esac
  case "$1" in
    --profile) profile="$2" ;;
    --ravel-image) ravel_image="$2" ;;
    --ravel-source-commit) ravel_source_commit="$2" ;;
    --bin-dir) bin_dir="$2" ;;
    --out-dir) out_dir="$2" ;;
    --anchor-ms) anchor_ms="$2" ;;
    --passes) passes="$2" ;;
    --reps) reps="$2" ;;
    --rss-band) bands+=("$2") ;;
    --diagnostic-prometheus-max-samples) diag_max_samples="$2" ;;
  esac
  shift 2
done

diagnostic=0
[ -n "$diag_max_samples" ] && diagnostic=1

[ -n "$profile" ] || usage_error "--profile is required"
[ -n "$bin_dir" ] || usage_error "--bin-dir is required"
[ -n "$out_dir" ] || usage_error "--out-dir is required"
if [ "$diagnostic" -eq 0 ]; then
  [ -n "$ravel_image" ] || usage_error "--ravel-image is required"
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
if [ "$diagnostic" -eq 1 ] && [ "$diag_max_samples" -eq 0 ]; then
  usage_error "--diagnostic-prometheus-max-samples must be at least 1"
fi

command -v python3 >/dev/null 2>&1 || {
  say "python3 is required"
  exit "$EXIT_STEP"
}

# --- refusals ----------------------------------------------------------------

if [ -n "$ravel_image" ] && ! [[ "$ravel_image" =~ @sha256:[0-9a-f]{64}$ ]]; then
  refuse "--ravel-image '$ravel_image' carries no @sha256: digest; a tag is a moving reference and the run could not say what it measured"
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
if [ "$diagnostic" -eq 1 ]; then
  # The falsification query only tests --query.max-samples on a profile whose
  # window the entry declares; elsewhere it loads a few samples, cannot reach
  # the limit, and a "success" would falsify nothing.
  entry_profiles="$(py corpus-entry-profiles "$CORPUS" "$FALSIFICATION_ENTRY")" ||
    refuse "the corpus has no entry ${FALSIFICATION_ENTRY}"
  case " ${entry_profiles} " in
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

if [ "$diagnostic" -eq 1 ]; then
  compose_files=(-f "${here}/docker-compose.yml" -f "${out_dir}/prometheus-max-samples.override.yml")
else
  compose_files=(-f "${here}/docker-compose.yml" -f "${here}/ravel.yml")
fi
# compose interpolates ravel.yml on every command, `ps` and `down` included.
export RAVEL_IMAGE="${ravel_image:-unset-diagnostic-run-starts-no-ravel}"

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
stamp_str ravel_image.reference "${ravel_image:-not given: diagnostic run}"
stamp_set ravel_image.resolved null
stamp_str ravel_source_commit "${ravel_source_commit:-not given: diagnostic run}"
stamp_str harness_commit "$(git_head "$bin_dir")"
stamp_str run_script_commit "$(git_head "$here")"
stamp_str bin_dir "$bin_dir"
stamp_str store_kind rustfs
stamp_str instance_type "$host_instance_type"
stamp_str host.arch "$(uname -m)"
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
stamp_set steps_completed '[]'
stamp_set failed_step null
stamp_set exit_code null
stamp_set teardown null

# --- teardown ----------------------------------------------------------------

STACK_UP=0
SAMPLER_PID=""
MEMORY_DONE=0
FAILED_STEP=""

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
  FAILED_STEP="$1"
  shift
  say "step $FAILED_STEP failed: $*"
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
  local name="$1" url="$2" i code
  for ((i = 1; i <= READY_TRIES; i++)); do
    code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$url" 2>/dev/null)" || code=""
    if [ "$code" = "200" ]; then
      return 0
    fi
    sleep "$READY_INTERVAL"
  done
  fail_step ready "$name did not answer 200 on $url after $READY_TRIES tries ${READY_INTERVAL}s apart (last status '${code}')"
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
    fail_step ingest "metricsbench_ingest exited $code; see ${out_dir}/ingest.log"
  fi
  step_done ingest
}

memory_step() {
  local code=0 verdict
  memory_summary || code=$?
  if [ "$code" -ne 0 ]; then
    FAILED_STEP="memory"
    verdict="$(cat "${out_dir}/memory-verdict.txt" 2>/dev/null)"
    say "step memory failed: ${verdict//$'\n'/; }"
    exit "$EXIT_MEMORY"
  fi
  step_done memory
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
  stamp_str ravel_catalog_fold "not applicable: diagnostic run starts no Ravel"
  stamp_str mimir_readiness "not applicable: diagnostic run starts no Mimir"

  wait_ready prometheus "${PROM_BASE}/-/ready"
  step_done ready
  start_sampler

  run_ingest --workload "$WORKLOAD" --profile "$profile" --anchor-ms "$anchor_ms" --store memory \
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
  resolved="$("$DOCKER" image inspect "$ravel_image" 2>/dev/null | py image-inspect "$ravel_image")" || code=$?
  [ -n "$resolved" ] && stamp_set ravel_image.resolved "$resolved"
  [ "$code" -eq 0 ] ||
    fail_step image "docker image inspect of $ravel_image did not list the digest the reference names"
  step_done image

  wait_ready prometheus "${PROM_BASE}/-/ready"
  wait_ready victoriametrics "${VM_BASE}/health"
  wait_ready mimir "${MIMIR_BASE}/ready"
  wait_ready ravel "${RAVEL_BASE}/api/v1/status/buildinfo"
  step_done ready

  start_sampler

  export "${RAVEL_TOKEN_ENV}=${RAVEL_TOKEN_VALUE}"
  run_ingest --workload "$WORKLOAD" --profile "$profile" --anchor-ms "$anchor_ms" --store memory \
    --tenant "$RAVEL_TENANT" \
    --ravel-endpoint "${RAVEL_BASE}/api/v1/write" --ravel-tenant "$RAVEL_TENANT" \
    --ravel-query-url "$RAVEL_BASE" --ravel-bearer-token-env "$RAVEL_TOKEN_ENV" \
    --prometheus-endpoint "${PROM_BASE}/api/v1/write" --prometheus-query-url "$PROM_BASE" \
    --victoriametrics-endpoint "${VM_BASE}/api/v1/write" --victoriametrics-query-url "${VM_BASE}/prometheus" \
    --mimir-endpoint "${MIMIR_BASE}/api/v1/push" --mimir-query-url "${MIMIR_BASE}/prometheus"

  line="$(py mimir-readiness "${out_dir}/ingest.json")" ||
    fail_step mimir-readiness "$line"
  stamp_str mimir_readiness "$line"
  step_done mimir-readiness

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
  local it_env=(env -u RAVEL_INSTANCE_TYPE)
  [ "$host_instance_type" = "unknown" ] || it_env=(env "RAVEL_INSTANCE_TYPE=${host_instance_type}")
  # ravel_git_commit names the source of the image under test, not this
  # checkout: GITHUB_SHA wins over git in metricsbench_report.
  code=0
  "${it_env[@]}" GITHUB_SHA="$ravel_source_commit" "${bin_dir}/metricsbench_report" provenance \
    --workload "$WORKLOAD" --corpus "$CORPUS" \
    --store-backend rustfs --region us-east-1 --endpoint http://rustfs:9000 \
    "${pins[@]}" \
    --config "ravel_image=${ravel_image}" \
    --config "ravel_source_commit=${ravel_source_commit}" \
    --config "harness_commit=$(py get "$STAMPS" harness_commit)" \
    --config "store_kind=rustfs" \
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
    --out "${out_dir}/report.json")
  [ -z "$passes" ] || query_args+=(--passes "$passes")
  [ -z "$reps" ] || query_args+=(--reps-per-pass "$reps")
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
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/validate.log"
    fail_step validate "metricsbench_report validate exited $code"
  fi
  step_done validate

  memory_step
}

if [ "$diagnostic" -eq 1 ]; then
  run_diagnostic
else
  run_full
fi
say "done: ${out_dir}"
exit 0
