#!/usr/bin/env bash
# Cases for run.sh, the MetricsBench end-to-end driver, with docker, curl,
# the three metricsbench bins and capture-effective-config.sh replaced by
# stubs that log every call. Nothing here starts a container.
#
#   a  success run                   -> exit 0; stamps, ingest, effective-config,
#                                       provenance, report and memory-summary
#                                       written; every step recorded; image
#                                       digest resolved; fold gauge and Mimir
#                                       readiness stamped; Ravel's token never
#                                       on a command line; torn down with
#                                       down -v
#   b  ingest fails                  -> exit 70, the message and stamps.json
#                                       name step ingest, no report.json, still
#                                       torn down
#   c  --ravel-image without digest  -> exit 65, nothing started, no stamps
#   d  --rss-band breach             -> exit 75, memory-summary.json records
#                                       the band as above, torn down
#   e  OOM-killed container          -> exit 75, memory-summary.json names it
#   f  diagnostic mode               -> exit 0, falsification.json carries the
#                                       HTTP status, errorType, error and
#                                       series count; no report.json; stamps
#                                       say diagnostic: true; Prometheus alone
#                                       started with the override; ingest into
#                                       Prometheus only
#   f2 diagnostic on a profile the   -> exit 65, nothing started, no stamps
#      entry is not declared for
#   g  --keep, --passes, --reps      -> exit 0, no down -v, stamps say kept;
#                                       --passes and --reps reach the query run
#   h  usage and refusals            -> 64 on an unknown flag, 65 on a missing
#                                       bin, an off-grid anchor, an out-dir
#                                       holding a report, leftover volumes
#   i  qualify fails                 -> exit 70, step qualify, torn down
#   j  no fold gauge on /metrics     -> stamps ravel_catalog_fold "absent"
#   k  capture exits 1 (partial)     -> exit 70, step capture, no query run,
#                                       torn down
#   l  a profile other than ci       -> exit 0, ingest runs without the
#                                       in-process row; stamps say skipped
#   m  ingest wall-time ceilings     -> each profile passes its
#                                       --max-ingest-secs (ci 600, cardinality
#                                       14400, history 72000, churn 18000), in
#                                       diagnostic mode too; no shard or batch
#                                       flag, so the bin's defaults of 50
#                                       shards and 2000 points apply; and its
#                                       --readback-timeout-secs (ci 30,
#                                       cardinality 900, history 300, churn
#                                       900)
#   n  RustFS log copy               -> teardown copies rustfs:/logs into
#                                       <out>/rustfs-logs before down -v, on
#                                       success (a) and on failure (b); a
#                                       failed copy is stamped and said, the
#                                       exit code stays 0, still torn down
#   o  descriptor limits             -> prometheus, victoriametrics, rustfs,
#                                       mimir (docker-compose.yml) and
#                                       ravel-server (ravel.yml) each carry
#                                       ulimits nofile soft and hard 524288,
#                                       read from the files' text
#
# Every JSON field is asserted by parsing it with python3 -I, not by grep.
#
# Run: bash deploy/metricsbench/tests/run.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/../run.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/metricsbench-run-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

pass() {
  passes=$((passes + 1))
  echo "PASS: $1"
}
fail() {
  fails=$((fails + 1))
  echo "FAIL: $1"
}

DIGEST="42116db1a8b2b0da52b5bc71219678d9b32d42aefb61ba46152ba9f9968d5ca7"
IMAGE="ghcr.io/nofireai/ravel-server:manual-0bf5fed@sha256:${DIGEST}"
COMMIT="0bf5fedb2834df7dc87f8086701dd289b049efb0"
ANCHOR=1760000010000

BIN="${TMP}/bin"
mkdir -p "${BIN}"

# --- stubs -------------------------------------------------------------------

# docker: compose up/ps/config/down/logs, stats, inspect, image inspect and
# volume ls, with the stack's up/down state in $STUB_STATE.
cat >"${BIN}/docker" <<'STUB'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"${STUB_LOG}"
running_services() {
  if [ -f "${STUB_STATE}.services" ]; then cat "${STUB_STATE}.services"; fi
}
case "$1" in
  compose)
    shift
    while [ "$1" = "-f" ]; do shift 2; done
    sub="$1"
    shift
    case "$sub" in
      up)
        [ "${STUB_UP_EXIT:-0}" -eq 0 ] || exit "${STUB_UP_EXIT}"
        echo up >"${STUB_STATE}"
        if [ "$#" -gt 1 ]; then
          shift
          printf '%s\n' "$@" >"${STUB_STATE}.services"
        else
          printf '%s\n' rustfs prometheus victoriametrics mimir ravel-server >"${STUB_STATE}.services"
          printf '%s\n' createbuckets ravel-qualify >"${STUB_STATE}.oneshots"
        fi
        ;;
      ps)
        [ "$(cat "${STUB_STATE}" 2>/dev/null)" = up ] || exit 0
        all=0
        [ "$1" = "-a" ] && all=1 && shift
        [ "$1" = "-q" ] && shift
        if [ -n "${1:-}" ]; then
          echo "id-$1"
          exit 0
        fi
        for s in $(running_services); do echo "id-$s"; done
        if [ "$all" -eq 1 ] && [ -f "${STUB_STATE}.oneshots" ]; then
          for s in $(cat "${STUB_STATE}.oneshots"); do echo "id-$s"; done
        fi
        ;;
      config)
        cat <<'JSON'
{"services": {"prometheus": {"image": "prom/prometheus@sha256:aaaa", "command": ["--config.file=/etc/prometheus/prometheus.yml", "--query.max-samples=50000000", "--web.enable-remote-write-receiver"]}}}
JSON
        ;;
      down)
        echo down >"${STUB_STATE}"
        ;;
      logs)
        echo "stub compose logs"
        ;;
      cp)
        if [ "${STUB_CP_EXIT:-0}" -ne 0 ]; then
          echo "stub: no such container path" >&2
          exit "${STUB_CP_EXIT}"
        fi
        mkdir -p "$2" && echo "stub rustfs log" >"$2/rustfs.log"
        ;;
    esac
    ;;
  stats)
    shift 4
    for id in "$@"; do
      s="${id#id-}"
      var="STUB_MEM_${s//-/_}"
      printf '{"Name":"metricsbench-%s-1","MemUsage":"%s / 7.6GiB"}\n' "$s" "${!var:-100MiB}"
    done
    ;;
  inspect)
    shift
    printf '['
    sep=""
    for id in "$@"; do
      s="${id#id-}"
      oom=false
      [ "${STUB_OOM_SERVICE:-}" = "$s" ] && oom=true
      st=running
      code=0
      case "$s" in createbuckets | ravel-qualify)
        st=exited
        [ "${STUB_QUALIFY_FAIL:-}" = "$s" ] && code=1
        ;;
      esac
      printf '%s{"Name":"/metricsbench-%s-1","Image":"sha256:%s","State":{"Status":"%s","ExitCode":%s,"OOMKilled":%s},"Config":{"Image":"stub/%s","Labels":{"com.docker.compose.service":"%s"}}}' \
        "$sep" "$s" "$s" "$st" "$code" "$oom" "$s" "$s"
      sep=","
    done
    printf ']\n'
    ;;
  image)
    ref="$3"
    printf '[{"Id":"sha256:imageid","RepoDigests":["%s@%s"]}]\n' "${ref%%[:@]*}" "${ref#*@}"
    ;;
  volume)
    [ -z "${STUB_LEFTOVER_VOLUME:-}" ] || echo "${STUB_LEFTOVER_VOLUME}"
    ;;
esac
exit 0
STUB

# curl: readiness 200, IMDS unreachable, /metrics with or without the fold
# gauge, and Prometheus' refusal of the falsification query.
cat >"${BIN}/curl" <<'STUB'
#!/usr/bin/env bash
printf 'curl %s\n' "$*" >>"${STUB_LOG}"
out=/dev/null
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift 2 ;;
    -w | -X | -H | --max-time | --data-urlencode) shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done
case "$url" in
  *imds.invalid*) exit 7 ;;
  */metrics)
    if [ -n "${STUB_NO_FOLD:-}" ]; then
      printf '# no fold yet\nravel_up 1\n' >"$out"
    else
      printf '# TYPE ravel_catalog_fold_last_success_timestamp_seconds gauge\nravel_catalog_fold_last_success_timestamp_seconds 1761000000\n' >"$out"
    fi
    printf 200
    ;;
  */api/v1/query)
    printf '{"status":"error","errorType":"execution","error":"query processing would load too many samples into memory in query execution"}' >"$out"
    printf 422
    ;;
  *) printf 200 ;;
esac
STUB

# metricsbench_ingest: an ingest report with a row per endpoint flag given.
cat >"${BIN}/metricsbench_ingest" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_ingest %s\n' "$*" >>"${STUB_LOG}"
printf 'metricsbench_ingest env token=%s\n' "${METRICSBENCH_RAVEL_TOKEN:+set}" >>"${STUB_LOG}"
if [ -n "${STUB_INGEST_EXIT:-}" ]; then
  echo "stub ingest: remote write refused" >&2
  exit "${STUB_INGEST_EXIT}"
fi
anchor=""
rows=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --anchor-ms) anchor="${args[$((i + 1))]}" ;;
    --*-endpoint)
      sys="${args[$i]#--}"
      sys="${sys%-endpoint}"
      rows="${rows}${rows:+,}{\"system\":\"${sys}\",\"readback\":{\"series_count\":1000,\"sample_count\":120000,\"matches_expected\":true,\"mismatches\":[]}}"
      ;;
  esac
done
printf '{"profile":{"name":"ci","scrape_interval_secs":15,"run":{"steps":120,"anchor_ms":%s}},"systems":[%s]}\n' "$anchor" "$rows"
STUB

cat >"${BIN}/metricsbench_query" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_query %s\n' "$*" >>"${STUB_LOG}"
printf 'metricsbench_query env token=%s\n' "${METRICSBENCH_RAVEL_TOKEN:+set}" >>"${STUB_LOG}"
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
  [ "${args[$i]}" = "--out" ] && printf '{"stub":"report"}\n' >"${args[$((i + 1))]}"
done
exit 0
STUB

cat >"${BIN}/metricsbench_report" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_report %s\n' "$*" >>"${STUB_LOG}"
case "$1" in
  provenance) printf '{"ravel_git_commit":"%s"}\n' "${GITHUB_SHA}" ;;
  validate) [ -s "$2" ] || exit 1 ;;
esac
exit 0
STUB

cat >"${BIN}/capture" <<'STUB'
#!/usr/bin/env bash
printf 'capture %s\n' "$*" >>"${STUB_LOG}"
[ -z "${STUB_CAPTURE_EXIT:-}" ] || exit "${STUB_CAPTURE_EXIT}"
cat >"$2" <<'JSON'
{"comparators": {
  "prometheus": {"version": "3.5.0", "image": {"container": "metricsbench-prometheus-1", "repo_digests": ["prom/prometheus@sha256:1111111111111111111111111111111111111111111111111111111111111111"]}},
  "victoriametrics": {"version": "v1.120.0", "image": {"container": "metricsbench-victoriametrics-1", "repo_digests": ["victoriametrics/victoria-metrics@sha256:2222222222222222222222222222222222222222222222222222222222222222"]}},
  "mimir": {"version": "2.16.0", "image": {"container": "metricsbench-mimir-1", "repo_digests": ["grafana/mimir@sha256:3333333333333333333333333333333333333333333333333333333333333333"]}},
  "ravel": {"version": "0.1.0", "image": {"container": "metricsbench-ravel-server-1", "repo_digests": ["ghcr.io/nofireai/ravel-server@sha256:42116db1a8b2b0da52b5bc71219678d9b32d42aefb61ba46152ba9f9968d5ca7"]}}
}}
JSON
STUB

chmod +x "${BIN}"/*

# run_case NAME [run.sh args...]: run with a fresh stub state and log; sets
# rc, OUT (the out-dir), LOG (every stub call) and ERR (run.sh's stderr).
run_case() {
  local name="$1"
  shift
  OUT="${TMP}/${name}/out"
  LOG="${TMP}/${name}/calls.log"
  ERR="${TMP}/${name}/stderr.log"
  mkdir -p "${TMP}/${name}"
  : >"${LOG}"
  rc=0
  PATH="${BIN}:${PATH}" \
    STUB_LOG="${LOG}" STUB_STATE="${TMP}/${name}/state" \
    METRICSBENCH_CAPTURE="${BIN}/capture" \
    METRICSBENCH_SAMPLE_INTERVAL=0.1 \
    METRICSBENCH_READY_TRIES=3 METRICSBENCH_READY_INTERVAL=0 \
    METRICSBENCH_IMDS="http://imds.invalid" \
    bash "${SCRIPT}" "$@" 2>"${ERR}" >/dev/null || rc=$?
}

full_args() {
  printf '%s\n' --profile ci --ravel-image "${IMAGE}" --ravel-source-commit "${COMMIT}" \
    --bin-dir "${BIN}" --out-dir "$1" --anchor-ms "${ANCHOR}"
}

# check NAME PYTHON-EXPR: evaluate a boolean over the parsed JSON files of
# $OUT (stamps, summary, falsification as available).
check() {
  local label="$1" expr="$2"
  if python3 -I - "${OUT}" "${expr}" <<'PY'; then
import json, os, sys
out, expr = sys.argv[1], sys.argv[2]
def j(name):
    with open(os.path.join(out, name)) as f:
        return json.load(f)
sys.exit(0 if eval(expr) else 1)
PY
    pass "${label}"
  else
    fail "${label}"
  fi
}

expect_rc() {
  if [ "${rc}" -eq "$2" ]; then
    pass "$1: exit $2"
  else
    fail "$1: exit ${rc}, want $2 (stderr: $(cat "${ERR}"))"
  fi
}

expect_file() {
  if [ -s "${OUT}/$2" ]; then pass "$1: $2 written"; else fail "$1: $2 missing"; fi
}

expect_no_file() {
  if [ ! -e "${OUT}/$2" ]; then pass "$1: no $2"; else fail "$1: $2 written"; fi
}

expect_log() {
  if grep -qF -- "$2" "${LOG}"; then pass "$1"; else fail "$1 (no '$2' in calls.log)"; fi
}

expect_no_log() {
  if grep -qF -- "$2" "${LOG}"; then fail "$1 ('$2' in calls.log)"; else pass "$1"; fi
}

expect_err() {
  if grep -qF -- "$2" "${ERR}"; then pass "$1"; else fail "$1 (no '$2' in stderr: $(cat "${ERR}"))"; fi
}

# expect_cp_before_down LABEL: teardown asked for RustFS's /logs, into the
# out-dir, before the down -v that destroys the container.
expect_cp_before_down() {
  local cp_line down_line
  cp_line="$(grep -nF -- "ravel.yml cp rustfs:/logs ${OUT}/rustfs-logs" "${LOG}" | head -n 1 | cut -d: -f1)"
  down_line="$(grep -nF -- " down -v" "${LOG}" | head -n 1 | cut -d: -f1)"
  if [ -n "${cp_line}" ] && [ -n "${down_line}" ] && [ "${cp_line}" -lt "${down_line}" ]; then
    pass "$1: RustFS's /logs copied before down -v"
  else
    fail "$1: RustFS's /logs not copied before down -v (cp line '${cp_line}', down line '${down_line}')"
  fi
}

# --- a: success --------------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/a/out")
run_case a "${args[@]}" --rss-band ravel-server:1:1000000000
expect_rc a 0
for f in stamps.json ingest.json effective-config.json provenance.json report.json memory-summary.json memory.jsonl compose.log; do
  expect_file a "$f"
done
check "a: stamps status passed, exit 0" 'j("stamps.json")["status"] == "passed" and j("stamps.json")["exit_code"] == 0'
check "a: every step recorded in order" 'j("stamps.json")["steps_completed"] == ["up","qualify","image","ready","sampler","ingest","mimir-readiness","capture","provenance","fold-state","query","validate","memory"]'
check "a: image reference and resolved digest stamped" 'j("stamps.json")["ravel_image"]["reference"] == "'"${IMAGE}"'" and j("stamps.json")["ravel_image"]["resolved"]["digest_matches_reference"] is True'
check "a: source commit, store kind, profile, anchor stamped" 'j("stamps.json")["ravel_source_commit"] == "'"${COMMIT}"'" and j("stamps.json")["store_kind"] == "rustfs" and j("stamps.json")["profile"] == "ci" and j("stamps.json")["anchor_ms"] == '"${ANCHOR}"' and j("stamps.json")["anchor_source"] == "flag"'
check "a: harness commit stamped (unavailable outside git)" 'j("stamps.json")["harness_commit"].startswith("unavailable")'
check "a: start and end times stamped" 'j("stamps.json")["started_at"].endswith("Z") and j("stamps.json")["ended_at"].endswith("Z")'
check "a: instance type unknown without IMDS" 'j("stamps.json")["instance_type"] == "unknown"'
check "a: comparator image digests from the capture" 'j("stamps.json")["comparator_images"]["mimir"]["repo_digests"][0].endswith("3333333333333333333333333333333333333333333333333333333333333333")'
check "a: fold gauge stamped" 'j("stamps.json")["ravel_catalog_fold"] == "1761000000"'
check "a: Mimir readiness line stamped" 'j("stamps.json")["mimir_readiness"].startswith("ingest read-back for mimir matched")'
check "a: memory summary has peak and final bytes, band ok" 'j("memory-summary.json")["containers"]["metricsbench-ravel-server-1"]["peak_bytes"] == 100*1024*1024 and j("memory-summary.json")["containers"]["metricsbench-ravel-server-1"]["final_bytes"] == 100*1024*1024 and j("memory-summary.json")["bands"][0]["verdict"] == "ok" and j("memory-summary.json")["oom_killed"] == []'
check "a: teardown stamped" 'j("stamps.json")["teardown"] == "docker compose down -v"'
expect_log "a: compose brought up with both files" "ravel.yml up -d"
expect_log "a: torn down with down -v" " down -v"
expect_log "a: ingest given all four endpoints" "--mimir-endpoint http://127.0.0.1:9009/api/v1/push"
expect_log "a: ingest gets Ravel's token by env var name" "--ravel-bearer-token-env METRICSBENCH_RAVEL_TOKEN"
expect_log "a: the token reached ingest's environment" "metricsbench_ingest env token=set"
expect_log "a: query given all four engines" "--engine name=mimir,url=http://127.0.0.1:9009/prometheus"
expect_log "a: query writes the diagnostics sidecar" "--diagnostics ${OUT}/diagnostics.json"
expect_log "a: report validated by path" "metricsbench_report validate ${OUT}/report.json"
expect_log "a: provenance pins the comparators from the capture" "--comparator mimir=2.16.0=sha256:3333"
expect_no_log "a: the token never on a command line" "metricsbench-token"
expect_log "a: ci keeps the in-process row on a memory store" "--store memory --tenant metricsbench"
check "a: stamps say the in-process row is present" 'j("stamps.json")["in_process_row"] == "present"'
expect_cp_before_down a
check "a: stamps say RustFS's logs were copied" 'j("stamps.json")["rustfs_logs"] == "copied to rustfs-logs/"'
expect_file a rustfs-logs/rustfs.log

# --- b: ingest fails ---------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/b/out")
STUB_INGEST_EXIT=1 run_case b "${args[@]}"
expect_rc b 70
expect_err "b: message names step ingest" "step ingest failed"
check "b: stamps name step ingest, status failed" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["status"] == "failed" and j("stamps.json")["exit_code"] == 70'
expect_no_file b report.json
expect_log "b: still torn down" " down -v"
expect_file b memory-summary.json
expect_cp_before_down b

# --- c: image without a digest -----------------------------------------------
run_case c --profile ci --ravel-image ghcr.io/nofireai/ravel-server:latest --ravel-source-commit "${COMMIT}" \
  --bin-dir "${BIN}" --out-dir "${TMP}/c/out"
expect_rc c 65
expect_err "c: refusal names the digest" "no @sha256: digest"
expect_no_log "c: nothing started" "compose -f"
expect_no_file c stamps.json

# --- d: rss band breach ------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/d/out")
STUB_MEM_ravel_server=2GiB run_case d "${args[@]}" --rss-band ravel-server:0:1073741824
expect_rc d 75
expect_err "d: message names the band" "--rss-band ravel-server:0:1073741824 above"
check "d: summary records the band above" 'j("memory-summary.json")["bands"][0]["verdict"] == "above" and j("memory-summary.json")["bands"][0]["peak_bytes"] == 2*1024**3 and j("memory-summary.json")["verdict"] == "band_violated"'
check "d: stamps name step memory, exit 75" 'j("stamps.json")["failed_step"] == "memory" and j("stamps.json")["exit_code"] == 75'
expect_log "d: torn down" " down -v"

# --- e: OOM kill -------------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/e/out")
STUB_OOM_SERVICE=mimir run_case e "${args[@]}"
expect_rc e 75
expect_err "e: message names the container" "metricsbench-mimir-1 was OOM-killed"
check "e: summary names the OOM-killed container" 'j("memory-summary.json")["oom_killed"] == ["metricsbench-mimir-1"] and j("memory-summary.json")["containers"]["metricsbench-mimir-1"]["oom_killed"] is True and j("memory-summary.json")["verdict"] == "oom_killed"'

# --- f: diagnostic -----------------------------------------------------------
run_case f --profile history --bin-dir "${BIN}" --out-dir "${TMP}/f/out" --anchor-ms "${ANCHOR}" \
  --diagnostic-prometheus-max-samples 1000
expect_rc f 0
expect_file f falsification.json
expect_file f memory-summary.json
expect_no_file f report.json
check "f: falsification records status, errorType, error, series count" 'j("falsification.json")["http_status"] == 422 and j("falsification.json")["errorType"] == "execution" and "too many samples" in j("falsification.json")["error"] and j("falsification.json")["result_series_count"] is None and j("falsification.json")["entry"] == "mb_long_range_7d_avg" and j("falsification.json")["query_max_samples"] == 1000'
check "f: queried at the newest sample" 'j("falsification.json")["time"] == "%d.000" % ((1760000010000 + 119*15000)//1000)'
check "f: stamps say diagnostic" 'j("stamps.json")["diagnostic"] is True and j("stamps.json")["status"] == "passed" and j("stamps.json")["prometheus_query_max_samples"] == 1000'
check "f: override sets --query.max-samples" '"--query.max-samples=1000" in j("prometheus-max-samples.override.yml")["services"]["prometheus"]["command"] and "--query.max-samples=50000000" not in j("prometheus-max-samples.override.yml")["services"]["prometheus"]["command"]'
expect_log "f: Prometheus started alone with the override" "prometheus-max-samples.override.yml up -d prometheus"
expect_log "f: ingest into Prometheus" "--prometheus-endpoint http://127.0.0.1:9090/api/v1/write"
expect_no_log "f: no other endpoint" "--ravel-endpoint"
expect_log "f: no in-process row" "--no-in-process-row"
expect_no_log "f: no memory store" "--store memory"
check "f: stamps say the in-process row is skipped" 'j("stamps.json")["in_process_row"] == "skipped"'
expect_no_log "f: no query run" "metricsbench_query"
expect_log "f: torn down" " down -v"
expect_no_log "f: no RustFS log copy without RustFS" " cp rustfs:/logs"
check "f: stamps say the RustFS log copy does not apply" 'j("stamps.json")["rustfs_logs"].startswith("not applicable")'

# --- f2: diagnostic on a profile the entry is not declared for ---------------
run_case f2 --profile ci --bin-dir "${BIN}" --out-dir "${TMP}/f2/out" --anchor-ms "${ANCHOR}" \
  --diagnostic-prometheus-max-samples 1000
expect_rc f2 65
expect_err "f2: refusal says it falsifies nothing" "falsifies nothing"
expect_no_log "f2: nothing started" "compose -f"
expect_no_file f2 stamps.json

# --- g: --keep ---------------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/g/out")
run_case g "${args[@]}" --keep --passes 2 --reps 1
expect_rc g 0
expect_log "g: --passes and --reps reach the query run" "--passes 2 --reps-per-pass 1"
check "g: stamps record passes and reps" 'j("stamps.json")["passes"] == "2" and j("stamps.json")["reps_per_pass"] == "1"'
expect_no_log "g: no down -v" " down -v"
check "g: stamps say kept" 'j("stamps.json")["teardown"] == "kept (--keep)"'

# --- h: usage and refusals ---------------------------------------------------
run_case h1 --profile ci --bogus
expect_rc "h unknown flag" 64
run_case h2 --profile ci --ravel-image "${IMAGE}" --ravel-source-commit "${COMMIT}" \
  --bin-dir "${TMP}/nowhere" --out-dir "${TMP}/h2/out"
expect_rc "h missing bin" 65
mapfile -t args < <(full_args "${TMP}/h3/out")
run_case h3 "${args[@]}" --anchor-ms 1760000010001
expect_rc "h off-grid anchor" 65
mkdir -p "${TMP}/h4/out"
echo '{}' >"${TMP}/h4/out/report.json"
mapfile -t args < <(full_args "${TMP}/h4/out")
run_case h4 "${args[@]}"
expect_rc "h out-dir holds a report" 65
mapfile -t args < <(full_args "${TMP}/h5/out")
STUB_LEFTOVER_VOLUME=metricsbench_rustfs-data run_case h5 "${args[@]}"
expect_rc "h leftover volume" 65
expect_no_log "h leftover volume: nothing started" " up -d"

# --- i: qualify fails --------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/i/out")
STUB_QUALIFY_FAIL=ravel-qualify run_case i "${args[@]}"
expect_rc i 70
check "i: stamps name step qualify" 'j("stamps.json")["failed_step"] == "qualify"'
expect_log "i: torn down" " down -v"

# --- j: no fold gauge --------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/j/out")
STUB_NO_FOLD=1 run_case j "${args[@]}"
expect_rc j 0
check "j: fold stamped absent" 'j("stamps.json")["ravel_catalog_fold"] == "absent"'

# --- k: partial capture ------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/k/out")
STUB_CAPTURE_EXIT=1 run_case k "${args[@]}"
expect_rc k 70
check "k: stamps name step capture" 'j("stamps.json")["failed_step"] == "capture"'
expect_no_log "k: no query run" "metricsbench_query"
expect_log "k: torn down" " down -v"

# --- l: a profile other than ci ----------------------------------------------
run_case l --profile history --ravel-image "${IMAGE}" --ravel-source-commit "${COMMIT}" \
  --bin-dir "${BIN}" --out-dir "${TMP}/l/out" --anchor-ms "${ANCHOR}"
expect_rc l 0
expect_log "l: ingest skips the in-process row" "--no-in-process-row"
expect_no_log "l: no memory store" "--store memory"
check "l: stamps say the in-process row is skipped" 'j("stamps.json")["in_process_row"] == "skipped"'

# --- m: ingest wall-time ceilings ----------------------------------------------
for triple in ci:600:30 cardinality:14400:900 history:72000:300 churn:18000:900; do
  name="${triple%%:*}"
  ceiling="${triple#*:}"
  readback="${ceiling#*:}"
  ceiling="${ceiling%%:*}"
  run_case "m-${name}" --profile "${name}" --ravel-image "${IMAGE}" \
    --ravel-source-commit "${COMMIT}" --bin-dir "${BIN}" --out-dir "${TMP}/m-${name}/out" \
    --anchor-ms "${ANCHOR}"
  expect_rc "m-${name}" 0
  expect_log "m-${name}: ingest gets the ${ceiling} s ceiling" "--max-ingest-secs ${ceiling} "
  expect_log "m-${name}: read-back gets the ${readback} s timeout" "--readback-timeout-secs ${readback} "
  expect_no_log "m-${name}: no shard count passed" "--http-shards"
  expect_no_log "m-${name}: no batch size passed" "--http-batch-size"
done
run_case m-diagnostic --profile history --bin-dir "${BIN}" --out-dir "${TMP}/m-diagnostic/out" \
  --anchor-ms "${ANCHOR}" --diagnostic-prometheus-max-samples 1000
expect_rc m-diagnostic 0
expect_log "m-diagnostic: the diagnostic ingest gets the history ceiling" "--max-ingest-secs 72000 "
expect_log "m-diagnostic: the diagnostic read-back gets the history timeout" "--readback-timeout-secs 300 "
# The defaults run.sh relies on by passing neither flag.
INGEST_SRC="${HERE}/../../../crates/ravel-bench/src/metrics_ingest.rs"
if grep -qF 'pub const DEFAULT_HTTP_SHARDS: usize = 50;' "${INGEST_SRC}"; then
  pass "m: the bin defaults to 50 shards"
else
  fail "m: DEFAULT_HTTP_SHARDS is not 50 in ${INGEST_SRC}"
fi
if grep -qF 'pub const DEFAULT_HTTP_BATCH_SIZE: usize = 2000;' "${INGEST_SRC}"; then
  pass "m: the bin defaults to 2000 points per request"
else
  fail "m: DEFAULT_HTTP_BATCH_SIZE is not 2000 in ${INGEST_SRC}"
fi

# --- n: RustFS log copy fails ----------------------------------------------------
mapfile -t args < <(full_args "${TMP}/n/out")
STUB_CP_EXIT=1 run_case n "${args[@]}"
expect_rc n 0
check "n: stamps say the copy failed, run passed" 'j("stamps.json")["rustfs_logs"].startswith("copy failed (exit 1)") and j("stamps.json")["status"] == "passed"'
expect_err "n: the failed copy is said" "copying RustFS's /logs failed (exit 1)"
expect_log "n: still torn down" " down -v"

# --- o: descriptor limits --------------------------------------------------------
# A service's block runs from its two-space key to the next key at two spaces
# or less; the four ulimits lines must appear in it, in order.
for pair in docker-compose.yml:prometheus docker-compose.yml:victoriametrics \
  docker-compose.yml:rustfs docker-compose.yml:mimir ravel.yml:ravel-server; do
  file="${pair%%:*}"
  svc="${pair#*:}"
  if python3 -I - "${HERE}/../${file}" "${svc}" <<'PY'; then
import re, sys
path, svc = sys.argv[1], sys.argv[2]
lines = open(path).read().splitlines()
start = next((i for i, l in enumerate(lines) if l == "  " + svc + ":"), None)
if start is None:
    sys.exit(1)
block = []
for l in lines[start + 1:]:
    if re.match(r"^ {0,2}[A-Za-z0-9_.-]+:", l):
        break
    block.append(l)
want = ["    ulimits:", "      nofile:", "        soft: 524288", "        hard: 524288"]
sys.exit(0 if any(block[i:i + 4] == want for i in range(len(block))) else 1)
PY
    pass "o: ${svc} in ${file} carries nofile 524288 soft and hard"
  else
    fail "o: ${svc} in ${file} lacks ulimits nofile soft and hard 524288"
  fi
done

echo
echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
