#!/usr/bin/env bash
# Cases for capture-effective-config.sh, run against stub HTTP servers that
# serve canned engine responses, and a stub docker binary.
#
#   a  all four comparators answer       -> exit 0, every version and the
#                                           Mimir engine fields present;
#                                           Ravel's runtime_flags unavailable
#                                           without docker
#   b  Mimir down                        -> exit 1, document still written,
#                                           mimir.version unavailable, the
#                                           other two complete
#   c  usage error                       -> exit 64
#   d  docker reachable (stub)           -> image digest and platform recorded,
#                                           Ravel's runtime_flags from .Config.Cmd
#   e  Mimir 3.x with no docker          -> engine default "mimir", image is
#                                           the docker-not-reachable string
#   f  daemon up, compose plugin broken  -> exit 0, image carries the compose
#                                           error, never "no running container"
#   g  buildinfo with data: null         -> exit 1, document written, version
#                                           unavailable, no crash
#   h  --out not writable                -> exit 70, no document
#   i  compose ps                        -> reads docker-compose.yml and
#                                           ravel.yml; RAVEL_IMAGE placeholder
#                                           when unset, passed on when set
#   j  Ravel 0.26.0 empty revision       -> exit 1, version kept, revision
#                                           unavailable
#
# Every field is asserted by parsing the JSON with python3 -I, not by grep.
#
# Run: bash deploy/metricsbench/tests/capture-effective-config.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/../capture-effective-config.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/capture-config-test.XXXXXX")"
server_pids=()
cleanup() {
  for pid in "${server_pids[@]}"; do
    kill "${pid}" 2>/dev/null
  done
  rm -rf "${TMP}"
}
trap cleanup EXIT

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

# stub_server <docroot>: serve <docroot> on a free loopback port and set
# stub_url to its base URL. Not called in a command substitution: the server
# would hold the substitution's pipe open, and its pid would be lost. The server writes its port to a file once bound, so no fixed port
# can collide with a running comparator.
cat >"${TMP}/stub_server.py" <<'PY'
import functools, http.server, sys
root, port_file = sys.argv[1], sys.argv[2]
handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=root)
handler.func.log_message = lambda *a, **k: None
srv = http.server.HTTPServer(("127.0.0.1", 0), handler)
with open(port_file + ".tmp", "w") as f:
    f.write(str(srv.server_address[1]))
import os
os.replace(port_file + ".tmp", port_file)
srv.serve_forever()
PY

stub_server() {
  local root="$1" port_file="$1.port" _
  python3 -I "${TMP}/stub_server.py" "${root}" "${port_file}" >/dev/null 2>&1 &
  server_pids+=("$!")
  for _ in $(seq 1 100); do
    [[ -s "${port_file}" ]] && break
    sleep 0.05
  done
  stub_url="http://127.0.0.1:$(cat "${port_file}")"
}

# A loopback port with nothing listening: bind, read the port, release.
dead_url() {
  echo "http://127.0.0.1:$(python3 -I -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
}

write() {
  mkdir -p "$(dirname "$1")"
  printf '%s' "$2" >"$1"
}

# --- canned responses -------------------------------------------------------
prom="${TMP}/prom"
write "${prom}/api/v1/status/buildinfo" '{"status":"success","data":{"version":"3.13.1","revision":"promrev123","branch":"HEAD","goVersion":"go1.25.1"}}'
write "${prom}/api/v1/status/flags" '{"status":"success","data":{"web.enable-remote-write-receiver":"true","storage.tsdb.retention.time":"45d"}}'
write "${prom}/api/v1/status/config" '{"status":"success","data":{"yaml":"global:\n  scrape_interval: 15s\nstorage:\n  tsdb:\n    out_of_order_time_window: 1h\n"}}'

vm="${TMP}/vm"
write "${vm}/metrics" '# HELP x
vm_cache_size_bytes 12
vm_app_version{version="victoria-metrics-20250331-131035-tags-v1.115.0-0-gabcdef1", short_version="v1.115.0"} 1
'
write "${vm}/flags" '-dedup.minScrapeInterval="0s"
-retentionPeriod="45d"
-storageDataPath="/victoria-metrics-data"
'
# What VictoriaMetrics really answers here; the script must not use it.
write "${vm}/api/v1/status/buildinfo" '{"status":"success","data":{"version":"2.24.0"}}'

mimir_config() {
  printf '%s' "target: all
querier:
  max_concurrent: 20
  query_engine: $1
  enable_query_engine_fallback: true
query_frontend:
  query_engine: other
"
}

mimir2="${TMP}/mimir2"
write "${mimir2}/prometheus/api/v1/status/buildinfo" '{"status":"success","data":{"application":"Grafana Mimir","version":"2.14.2","revision":"mimirrev214","branch":"release-2.14","goVersion":"go1.22.7","features":{}}}'
write "${mimir2}/config" "$(mimir_config prometheus)"

mimir3="${TMP}/mimir3"
write "${mimir3}/prometheus/api/v1/status/buildinfo" '{"status":"success","data":{"application":"Grafana Mimir","version":"3.2.0","revision":"mimirrev320","branch":"release-3.2","goVersion":"go1.25.1","features":{}}}'
write "${mimir3}/config" "$(mimir_config mimir)"

# Ravel's buildinfo, with a revision, and as the 0.26.0 image answers it:
# every build field but version empty.
ravel="${TMP}/ravel"
write "${ravel}/api/v1/status/buildinfo" '{"status":"success","data":{"version":"0.27.0","revision":"ravelrev027","branch":"","buildUser":"","buildDate":"","goVersion":""}}'
ravel026="${TMP}/ravel026"
write "${ravel026}/api/v1/status/buildinfo" '{"status":"success","data":{"version":"0.26.0","revision":"","branch":"","buildUser":"","buildDate":"","goVersion":""}}'

stub_server "${prom}"
prom_url="${stub_url}"
stub_server "${ravel}"
ravel_url="${stub_url}"
stub_server "${ravel026}"
ravel026_url="${stub_url}"
stub_server "${vm}"
vm_url="${stub_url}"
stub_server "${mimir2}"
mimir2_url="${stub_url}"
stub_server "${mimir3}"
mimir3_url="${stub_url}"

# Docker stubs: one unreachable, one answering for the compose services.
mkdir -p "${TMP}/bin"
write "${TMP}/bin/docker-down" '#!/usr/bin/env bash
echo "Cannot connect to the Docker daemon" >&2
exit 1
'
# shellcheck disable=SC2016 # the stub body expands when the stub runs
write "${TMP}/bin/docker-up" '#!/usr/bin/env bash
case "$1 $2" in
  "info --format") echo 27.0.0 ;;
  "compose -f")
    printf "%s\n" "$*" "RAVEL_IMAGE=${RAVEL_IMAGE-<unset>}" >"$(dirname "$0")/compose-call.log"
    echo "{\"ID\":\"c-prom\",\"Service\":\"prometheus\"}"
    echo "{\"ID\":\"c-vm\",\"Service\":\"victoriametrics\"}"
    echo "{\"ID\":\"c-mimir\",\"Service\":\"mimir\"}"
    echo "{\"ID\":\"c-ravel\",\"Service\":\"ravel-server\"}" ;;
  "inspect c-mimir")
    echo "[{\"Image\":\"sha256:img-mimir\",\"Config\":{\"Image\":\"grafana/mimir:2.14.2@sha256:2d39\"}}]" ;;
  "inspect c-ravel")
    echo "[{\"Image\":\"sha256:img-ravel\",\"Config\":{\"Image\":\"ghcr.io/nofireai/ravel-server:0.26.0@sha256:b299\",\"Entrypoint\":[\"/usr/local/bin/ravel-server\"],\"Cmd\":[\"--mode\",\"all\",\"--max-ingest-lag\",\"45d\"]}}]" ;;
  "inspect "*)
    echo "[{\"Image\":\"sha256:img-$2\",\"Config\":{\"Image\":\"ref-$2\"}}]" ;;
  "image inspect")
    echo "[{\"RepoDigests\":[\"repo@sha256:digest-$3\"],\"Architecture\":\"arm64\",\"Os\":\"linux\"}]" ;;
  *) exit 1 ;;
esac
'
chmod +x "${TMP}/bin/docker-down" "${TMP}/bin/docker-up"

# check <case> <json-file>: each line of stdin is
# "<python expression over d> == <python literal>"; reports every mismatch.
check() {
  local name="$1" file="$2" rc=0
  cat >"${TMP}/expect.txt"
  python3 -I - "${file}" "${TMP}/expect.txt" <<'PY' || rc=$?
import json, re, sys
d = json.load(open(sys.argv[1]))
bad = 0
for line in open(sys.argv[2]).read().splitlines():
    if not line.strip():
        continue
    expr, want = line.split(" == ", 1)
    try:
        got = eval(expr, {"d": d, "re": re, "json": json})
    except Exception as e:
        got = f"<{type(e).__name__}: {e}>"
    if got != eval(want):
        print(f"  {expr}: got {got!r}, want {want}")
        bad = 1
sys.exit(bad)
PY
  if [[ ${rc} -eq 0 ]]; then pass "${name}"; else fail "${name}"; fi
}

expect_exit() {
  local name="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass "${name}: exit ${got}"
  else
    fail "${name}: exit ${got}, want ${want}"
  fi
}

# --- a: every comparator answers ---------------------------------------------
out_a="${TMP}/a.json"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-down" "${SCRIPT}" --out "${out_a}" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/a.err" || code=$?
expect_exit "a all answer" 0 "${code}"
check "a fields" "${out_a}" <<EOF
d["comparators"]["prometheus"]["version"] == "3.13.1"
d["comparators"]["prometheus"]["revision"] == "promrev123"
d["comparators"]["prometheus"]["runtime_flags"]["storage.tsdb.retention.time"] == "45d"
d["comparators"]["prometheus"]["config_settings"]["storage.tsdb.out_of_order_time_window"] == "1h"
d["urls_queried"]["prometheus"]["config_settings"] == "${prom_url}/api/v1/status/config"
d["comparators"]["victoriametrics"]["version"] == "v1.115.0"
d["comparators"]["victoriametrics"]["revision"] == "victoria-metrics-20250331-131035-tags-v1.115.0-0-gabcdef1"
d["comparators"]["victoriametrics"]["runtime_flags"]["retentionPeriod"] == "45d"
d["comparators"]["mimir"]["version"] == "2.14.2"
d["comparators"]["mimir"]["revision"] == "mimirrev214"
d["comparators"]["mimir"]["querier_query_engine"] == "prometheus"
d["comparators"]["mimir"]["query_engine_default_for_version"] == "prometheus"
d["comparators"]["mimir"]["runtime_flags"]["format"] == "yaml"
("query_engine: prometheus" in d["comparators"]["mimir"]["runtime_flags"]["effective_config"]) == True
d["comparators"]["mimir"]["image"] == "unavailable: docker not reachable"
d["urls_queried"]["mimir"]["buildinfo"] == "${mimir2_url}/prometheus/api/v1/status/buildinfo"
d["urls_queried"]["mimir"]["runtime_flags"] == "${mimir2_url}/config"
d["urls_queried"]["victoriametrics"]["buildinfo"] == "${vm_url}/metrics"
d["urls_queried"]["prometheus"]["runtime_flags"] == "${prom_url}/api/v1/status/flags"
d["comparators"]["ravel"]["version"] == "0.27.0"
d["comparators"]["ravel"]["revision"] == "ravelrev027"
d["urls_queried"]["ravel"]["buildinfo"] == "${ravel_url}/api/v1/status/buildinfo"
("docker not reachable" in d["comparators"]["ravel"]["runtime_flags"]["unavailable"]) == True
d["comparators"]["ravel"]["image"] == "unavailable: docker not reachable"
len(d["script_revision"]) == 40
bool(re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", d["captured_at"])) == True
EOF

# --- b: Mimir down ------------------------------------------------------------
out_b="${TMP}/b.json"
mimir_dead="$(dead_url)"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-down" "${SCRIPT}" --out "${out_b}" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir_dead}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/b.err" || code=$?
expect_exit "b mimir down" 1 "${code}"
if [[ -s "${out_b}" ]]; then pass "b document written"; else fail "b document written"; fi
check "b fields" "${out_b}" <<EOF
sorted(d["comparators"]["mimir"]["version"]) == ["unavailable"]
d["comparators"]["mimir"]["version"]["unavailable"].startswith("GET ${mimir_dead}/prometheus/api/v1/status/buildinfo: ") == True
sorted(d["comparators"]["mimir"]["revision"]) == ["unavailable"]
sorted(d["comparators"]["mimir"]["runtime_flags"]) == ["unavailable"]
sorted(d["comparators"]["mimir"]["querier_query_engine"]) == ["unavailable"]
d["comparators"]["mimir"]["query_engine_default_for_version"] == {"unavailable": "mimir version unavailable"}
d["comparators"]["prometheus"]["version"] == "3.13.1"
d["comparators"]["prometheus"]["revision"] == "promrev123"
d["comparators"]["victoriametrics"]["version"] == "v1.115.0"
d["comparators"]["victoriametrics"]["runtime_flags"]["storageDataPath"] == "/victoria-metrics-data"
EOF

# --- c: usage -----------------------------------------------------------------
code=0
"${SCRIPT}" --no-such-flag >/dev/null 2>&1 || code=$?
expect_exit "c unknown flag" 64 "${code}"
code=0
"${SCRIPT}" --mimir >/dev/null 2>&1 || code=$?
expect_exit "c flag without value" 64 "${code}"

# --- d: docker reachable --------------------------------------------------------
out_d="${TMP}/d.json"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-up" "${SCRIPT}" --out "${out_d}" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/d.err" || code=$?
expect_exit "d docker up" 0 "${code}"
check "d image fields" "${out_d}" <<EOF
d["comparators"]["mimir"]["image"]["container"] == "c-mimir"
d["comparators"]["mimir"]["image"]["image_id"] == "sha256:img-mimir"
d["comparators"]["mimir"]["image"]["config_image"] == "grafana/mimir:2.14.2@sha256:2d39"
d["comparators"]["mimir"]["image"]["repo_digests"] == ["repo@sha256:digest-sha256:img-mimir"]
d["comparators"]["mimir"]["image"]["architecture"] == "arm64"
d["comparators"]["mimir"]["image"]["os"] == "linux"
d["comparators"]["prometheus"]["image"]["config_image"] == "ref-c-prom"
d["comparators"]["ravel"]["image"]["container"] == "c-ravel"
d["comparators"]["ravel"]["image"]["config_image"] == "ghcr.io/nofireai/ravel-server:0.26.0@sha256:b299"
d["comparators"]["ravel"]["runtime_flags"]["cmd"] == ["--mode", "all", "--max-ingest-lag", "45d"]
d["comparators"]["ravel"]["runtime_flags"]["source"] == "docker inspect .Config.Cmd of compose service ravel-server"
EOF

# --- i: compose ps reads ravel.yml, with RAVEL_IMAGE placeholder or passed on ---
# ravel.yml refuses interpolation without RAVEL_IMAGE, so the listing would
# fail for every service if the script left it unset.
compose_log="${TMP}/bin/compose-call.log"
if grep -q -- "-f .*/docker-compose.yml -f .*/ravel.yml ps --format json" "${compose_log}"; then
  pass "i compose ps reads both files"
else
  fail "i compose ps reads both files: $(cat "${compose_log}")"
fi
if grep -qx "RAVEL_IMAGE=unset-placeholder-for-compose-ps" "${compose_log}"; then
  pass "i placeholder RAVEL_IMAGE when unset"
else
  fail "i placeholder RAVEL_IMAGE when unset: $(cat "${compose_log}")"
fi
code=0
RAVEL_IMAGE="ghcr.io/nofireai/ravel-server:0.26.0@sha256:given" \
  METRICSBENCH_DOCKER="${TMP}/bin/docker-up" "${SCRIPT}" --out "${TMP}/i.json" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/i.err" || code=$?
expect_exit "i RAVEL_IMAGE set" 0 "${code}"
if grep -qx "RAVEL_IMAGE=ghcr.io/nofireai/ravel-server:0.26.0@sha256:given" "${compose_log}"; then
  pass "i RAVEL_IMAGE passed on unchanged"
else
  fail "i RAVEL_IMAGE passed on unchanged: $(cat "${compose_log}")"
fi

# --- j: Ravel 0.26.0 answers an empty revision -----------------------------------
# The pinned image's buildinfo carries no revision: that is a partial capture
# (exit 1) with the version kept, not a pass with an empty string recorded.
out_j="${TMP}/j.json"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-down" "${SCRIPT}" --out "${out_j}" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel026_url}" \
  2>"${TMP}/j.err" || code=$?
expect_exit "j ravel empty revision" 1 "${code}"
if grep -q "unavailable for: ravel$" "${TMP}/j.err"; then
  pass "j names ravel"
else
  fail "j names ravel: $(cat "${TMP}/j.err")"
fi
check "j fields" "${out_j}" <<EOF
d["comparators"]["ravel"]["version"] == "0.26.0"
d["comparators"]["ravel"]["revision"] == {"unavailable": "data.revision missing or empty in response from ${ravel026_url}/api/v1/status/buildinfo"}
d["comparators"]["mimir"]["revision"] == "mimirrev214"
EOF

# --- e: Mimir 3.x ---------------------------------------------------------------
out_e="${TMP}/e.json"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-down" "${SCRIPT}" --out "${out_e}" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir3_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/e.err" || code=$?
expect_exit "e mimir 3.x" 0 "${code}"
check "e engine fields" "${out_e}" <<EOF
d["comparators"]["mimir"]["version"] == "3.2.0"
d["comparators"]["mimir"]["querier_query_engine"] == "mimir"
d["comparators"]["mimir"]["query_engine_default_for_version"] == "mimir"
d["comparators"]["victoriametrics"]["image"] == "unavailable: docker not reachable"
EOF

# --- f: docker daemon up, compose plugin broken ---------------------------------
# The real cause must reach the document; "no running container" would be a
# false statement about a stack that is up and answering.
# shellcheck disable=SC2016 # the stub body expands when the stub runs
write "${TMP}/bin/docker-compose-broken" '#!/usr/bin/env bash
case "$1 $2" in
  "info --format") echo 27.0.0 ;;
  "compose -f") echo "permission denied while trying to connect to the compose plugin" >&2; exit 1 ;;
  *) exit 1 ;;
esac
'
chmod +x "${TMP}/bin/docker-compose-broken"
out_f="${TMP}/f.json"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-compose-broken" "${SCRIPT}" --out "${out_f}" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/f.err" || code=$?
expect_exit "f compose broken" 0 "${code}"
check "f image reason" "${out_f}" <<EOF
d["comparators"]["prometheus"]["image"].startswith("unavailable: docker compose exited 1: permission denied") == True
d["comparators"]["mimir"]["image"] == d["comparators"]["prometheus"]["image"]
d["comparators"]["ravel"]["image"] == d["comparators"]["prometheus"]["image"]
("permission denied" in d["comparators"]["ravel"]["runtime_flags"]["unavailable"]) == True
("no running container" in json.dumps(d)) == False
EOF

# --- g: a comparator answers buildinfo with data: null ---------------------------
# A malformed answer is a partial capture (exit 1, document written), not a
# crash: nothing may abort before the document exists.
prom_null="${TMP}/prom_null"
write "${prom_null}/api/v1/status/buildinfo" '{"status":"success","data":null}'
write "${prom_null}/api/v1/status/flags" '{"status":"success","data":{"x":"y"}}'
stub_server "${prom_null}"
prom_null_url="${stub_url}"
out_g="${TMP}/g.json"
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-down" "${SCRIPT}" --out "${out_g}" \
  --prometheus "${prom_null_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/g.err" || code=$?
expect_exit "g data null" 1 "${code}"
if [[ -s "${out_g}" ]]; then pass "g document written"; else fail "g document written"; fi
check "g fields" "${out_g}" <<EOF
sorted(d["comparators"]["prometheus"]["version"]) == ["unavailable"]
("not buildinfo JSON" in d["comparators"]["prometheus"]["version"]["unavailable"]) == True
sorted(d["comparators"]["prometheus"]["config_settings"]) == ["unavailable"]
d["comparators"]["mimir"]["version"] == "2.14.2"
EOF

# --- h: the document cannot be written -------------------------------------------
# Exit 1 means "document written, partial"; an unwritable --out is a crash and
# gets its own code, so a caller never cites a stale file as this run's capture.
code=0
METRICSBENCH_DOCKER="${TMP}/bin/docker-down" "${SCRIPT}" --out "${TMP}/no-such-dir/h.json" \
  --prometheus "${prom_url}" --victoriametrics "${vm_url}" --mimir "${mimir2_url}" \
  --ravel "${ravel_url}" \
  2>"${TMP}/h.err" || code=$?
expect_exit "h unwritable out" 70 "${code}"
if [[ -e "${TMP}/no-such-dir/h.json" ]]; then fail "h no document"; else pass "h no document"; fi

if [[ -n "${CAPTURE_TEST_KEEP_CASE_A:-}" ]]; then
  cp "${out_a}" "${CAPTURE_TEST_KEEP_CASE_A}"
fi

echo
echo "${passes} passed, ${fails} failed"
[[ ${fails} -eq 0 ]]
