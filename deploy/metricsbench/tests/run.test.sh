#!/usr/bin/env bash
# Cases for run.sh, the MetricsBench end-to-end driver, with docker, curl,
# uname, sha256sum, shasum, the three metricsbench bins and
# capture-effective-config.sh replaced by stubs that log every call. Nothing
# here starts a container or downloads anything. The sha256sum and shasum
# stubs answer run.sh's pinned s3s-fs constant for the bytes the curl stub
# serves as that arch's asset, and the real hash for anything else, so the
# comparison under test is run.sh's own.
#
#   a  success run                   -> exit 0; stamps, ingest, effective-config,
#                                       provenance, report and memory-summary
#                                       written; every step recorded; image
#                                       digest resolved; fold gauge and Mimir
#                                       readiness stamped; Ravel's token never
#                                       on a command line; torn down with
#                                       down -v
#   b  ingest fails, no report       -> exit 70, the message and stamps.json
#                                       name step ingest, no report.json, no
#                                       query run, still torn down
#   q  a row's read-back fails       -> exit 70 naming ingest; every later step
#                                       still runs, query with
#                                       --diagnostic-after-failed-ingest, validate
#                                       refuses the report; mimir-readiness is
#                                       stamped, not checked
#   q2 a row stalls                  -> the same, exit 70 naming ingest
#   q3 q plus an --rss-band breach   -> exit 70 naming ingest, memory listed in
#                                       failed_after_ingest and
#                                       deferred_failures, after stop, fold,
#                                       start and readback-folded ran
#   q4 the oracle's read-back fails, -> exit 70 naming ingest at query: query
#      plus an --rss-band breach        refuses, no report.json, no validate;
#                                       only query in failed_after_ingest, the
#                                       breach recorded in memory-summary.json
#   r  ingest exits non-zero, every  -> exit 70 naming ingest at once, no
#      row verified                     capture, no query
#   c  --ravel-image without digest  -> exit 65, nothing started, no stamps
#   d  --rss-band breach             -> exit 75 naming memory, memory-summary.json
#                                       records the band as above, memory in
#                                       deferred_failures, stop, fold, start
#                                       and readback-folded ran first, torn
#                                       down
#   e  OOM-killed container          -> the same, memory-summary.json names it
#   e2 d plus a failed folded        -> exit 70 naming readback-folded, memory
#      read-back                        in deferred_failures, the message names
#                                       both
#   e3 diagnostic mode plus an       -> exit 75 at once, nothing deferred
#      --rss-band breach
#   e4 u7 plus an --rss-band breach  -> the four skipped, then exit 70 naming
#                                       ingest, memory in failed_after_ingest
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
#   n  s3s-fs fetch                  -> a (empty cache): the x86_64 asset is
#                                       downloaded, cached executable, stamped
#                                       (object_store) and its path reaches
#                                       compose up as S3S_FS_BIN; n1: a cached
#                                       binary with the pinned sha256 is
#                                       reused, no download; n2: a download
#                                       with another sha256 exits 65, step
#                                       fetch-s3s-fs, nothing cached or
#                                       started; n3: a cached binary with
#                                       another sha256 exits 65, no download,
#                                       nothing started; n4: an engine arch
#                                       other than amd64/arm64 exits 65 before
#                                       stamps; n5: with the engine silent,
#                                       uname -m aarch64 fetches the aarch64
#                                       asset; n6: a failed download exits 70;
#                                       n7: an arm64 engine on a host whose
#                                       uname -m says arm64 (macOS) fetches
#                                       aarch64; n8: an amd64 engine fetches
#                                       x86_64 whatever uname -m says; n9: no
#                                       sha256sum, shasum -a 256 hashes; n10:
#                                       the same refuses a mismatch with 65;
#                                       n11: neither tool exits 65 before
#                                       stamps; f: a diagnostic run fetches
#                                       nothing
#   p  local-build image           -> --ravel-image REF --ravel-image-id ID passes
#                                       when docker image inspect reports Id ID;
#                                       stamps and provenance say local-build
#                                       with the Id; refused 65 before anything
#                                       starts when the Id differs, and with a
#                                       profile other than ci; both forms, or
#                                       neither, or an id without
#                                       --ravel-image, exit 64
#   o  descriptor limits             -> prometheus, victoriametrics, s3s,
#                                       mimir (docker-compose.yml) and
#                                       ravel-server (ravel.yml) each carry
#                                       ulimits nofile soft and hard 524288,
#                                       read from the files' text
#
#   s  the real-S3 lane              -> s: exit 0; compose takes real-s3.yml with
#                                       the prefix and region exported, no
#                                       s3s-fs download, stamps and provenance
#                                       name real S3 and billed requests; s1:
#                                       without the flags none of that happens;
#                                       s2-s7: a lone flag, a prefix S3 cannot
#                                       name or past 44 characters, a malformed
#                                       region and the falsification run exit 64,
#                                       and the usage names the flags; s8: an
#                                       engine arch with no s3s-fs asset passes
#   t  real S3 with a local build    -> exit 65, nothing started
#   u  stop, fold, start, folded     -> u (case a's output): fold.out,
#      read-back                        readback-folded.json and both /metrics
#                                       scrapes written, the three stamps, the
#                                       calls in order memory, stop, fold,
#                                       start, readiness, scrape, read-back,
#                                       scrape, the fold run while ravel-server
#                                       was stopped; u1: a fold that sealed
#                                       nothing and u2: a watermark before the
#                                       ingest end hour exit 70 naming fold,
#                                       with no start; u2a: a failed stop and
#                                       u2b: a failed start exit 70 naming
#                                       their step; u3: a read-back-only exit
#                                       1 and u4: a failed scrape exit 70 naming
#                                       readback-folded; u5: Ravel's ingest
#                                       read-back failed, the four still run,
#                                       exit 70 naming ingest; u6: the same with
#                                       readback-folded listed after ingest; u7:
#                                       a Ravel row_failure and u8: a Ravel
#                                       stall skip them, stamped why; u9: the
#                                       diagnostic mode runs none; u10: the
#                                       real-S3 lane folds with its overlay;
#                                       u11: large fetch misses leave few
#                                       catalog GETs, a pass; u12: one catalog
#                                       GET per listed data object exits 70
#                                       naming readback-folded where the flush
#                                       counters would not, u13: listed after a
#                                       failed ingest; u14: no fetch-miss
#                                       counter, u15a: no ravel-objects.json
#                                       and u15b: no data.count in it exit 70
#                                       as could not measure; u15: no flush
#                                       counter stamps the cross-check null
#                                       and passes; u16: --folded-catalog-get-
#                                       max-fraction 1 passes u13's figures as
#                                       not discriminating; u17-u19: a
#                                       malformed or out-of-range fraction
#                                       exits 64; u20: a ci-sized run whose
#                                       limit is above its listed object count
#                                       passes as not discriminating; u21: a
#                                       cardinality-sized run within the floor
#                                       plus fraction passes, u22: above it
#                                       exits 70; u23: --folded-catalog-get-
#                                       floor 0 is stamped; u24-u27: a floor
#                                       that is not a whole number exits 64
#   w  scrapes around the query    -> w (case a's output): Ravel's and Mimir's
#      step                             /metrics before and after the query
#                                       step written, query_scrapes stamps ok
#                                       for all four, the before pair ahead of
#                                       the query run and the after pair
#                                       after it and ahead of validate and the
#                                       stop step; w1: Mimir refusing the
#                                       connection, w2: answering 503 and w3:
#                                       an empty body exit 0, stamped why,
#                                       with no Mimir files; w4: the same with
#                                       an --rss-band breach exits 75 as d;
#                                       q4: a refused query takes no after
#                                       pair, stamped not taken; f: the
#                                       diagnostic mode scrapes neither,
#                                       stamped not applicable
#   v  Ravel's objects               -> v: a listing of every kind splits into
#                                       data, commit, qualify and other with
#                                       their sum as total, stamped the same,
#                                       no band; the portable stack lists
#                                       ravel-metricsbench through
#                                       http://s3s:9000; the step sits
#                                       between mimir-readiness and capture;
#                                       v1: a band holding the data count
#                                       passes; v2: one holding the total and
#                                       the m/ count but not the data count
#                                       exits 70 naming objects; v3: a
#                                       malformed or inverted band exits 64;
#                                       v4: a band in the diagnostic mode
#                                       exits 64; v5: after a failed ingest
#                                       verification a band breach is listed
#                                       in failed_after_ingest; v6: a failed
#                                       listing and v7: a truncated one exit
#                                       70; v8: the real-S3 lane lists
#                                       PREFIX-ravel with no endpoint
#                                       override; f: the diagnostic mode runs
#                                       no objects step
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
REAL_UNAME="$(command -v uname)"

S3S_RELEASE="s3s-fs-0.17.0-ravel.3"
S3S_SHA_X86_64="99592a8a64cbb773c34aee9966537eb4d1091d9a91b70699220f1f34dd4b384e"
S3S_SHA_AARCH64="92d7bbbb29bd4c5aee00b5f1c280c1402e799ed8a28f497e5ba813f1ebc3eecc"

# --- stubs -------------------------------------------------------------------

# docker: version (Server.Arch is STUB_ENGINE_ARCH, default amd64; exit 1
# with STUB_ENGINE_DOWN), compose up/ps/config/down/logs/stop/start (each up
# and start also logged as a docker-compose-up/-start line), stats,
# inspect, image inspect and volume ls, with the stack's up/down state in
# $STUB_STATE and ravel-server's stopped/running state in $STUB_STATE.ravel.
cat >"${BIN}/docker" <<'STUB'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >>"${STUB_LOG}"
running_services() {
  if [ -f "${STUB_STATE}.services" ]; then cat "${STUB_STATE}.services"; fi
}
case "$1" in
  version)
    if [ -n "${STUB_ENGINE_DOWN:-}" ]; then
      echo "Cannot connect to the Docker daemon" >&2
      exit 1
    fi
    echo "${STUB_ENGINE_ARCH:-amd64}"
    ;;
  compose)
    shift
    while [ "$1" = "-f" ]; do shift 2; done
    sub="$1"
    shift
    case "$sub" in
      up | start)
        printf 'docker-compose-%s %s\n' "$sub" "$*" >>"${STUB_LOG}"
        ;;
    esac
    # Once ravel-server has been stopped, a start or up of it reruns its
    # one-shot dependencies unless the up carries --no-deps, and on the
    # real-S3 lane createbuckets refuses the bucket it already filled: the
    # stub fails that call the way compose did there. --no-deps alone starts
    # the stopped container, exit STUB_START_EXIT.
    if [ "$(cat "${STUB_STATE}.ravel" 2>/dev/null)" = stopped ] &&
      { [ "$sub" = start ] || [ "$sub" = up ]; } && [ "${!#}" = ravel-server ]; then
      case " $* " in
        *" --no-deps "*) ;;
        *)
          printf 'docker-oneshot started %s\n' createbuckets ravel-qualify >>"${STUB_LOG}"
          echo 'service "createbuckets" didn'"'"'t complete successfully: exit 1' >&2
          exit 1
          ;;
      esac
      [ "${STUB_START_EXIT:-0}" -eq 0 ] || exit "${STUB_START_EXIT}"
      echo running >"${STUB_STATE}.ravel"
      exit 0
    fi
    case "$sub" in
      up)
        [ "${STUB_UP_EXIT:-0}" -eq 0 ] || exit "${STUB_UP_EXIT}"
        printf 'docker-env up S3S_FS_BIN=%s\n' "${S3S_FS_BIN-<unset>}" >>"${STUB_LOG}"
        printf 'docker-env up S3_PREFIX=%s S3_REGION=%s\n' "${METRICSBENCH_S3_BUCKET_PREFIX-<unset>}" "${METRICSBENCH_S3_REGION-<unset>}" >>"${STUB_LOG}"
        echo up >"${STUB_STATE}"
        if [ "$#" -gt 1 ]; then
          shift
          printf '%s\n' "$@" >"${STUB_STATE}.services"
        else
          printf '%s\n' s3s prometheus victoriametrics mimir ravel-server >"${STUB_STATE}.services"
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
      # stop and start: ravel-server's state for the fold to log, exit
      # STUB_STOP_EXIT / STUB_START_EXIT (a start of a stopped ravel-server
      # is handled above). On stop, STUB_OBJECTS_DOC_AT (a
      # ravel-objects.json path) is replaced by STUB_OBJECTS_DOC, or removed
      # when that is empty, after the objects step wrote it.
      stop)
        [ "${STUB_STOP_EXIT:-0}" -eq 0 ] || exit "${STUB_STOP_EXIT}"
        [ "$1" = ravel-server ] && echo stopped >"${STUB_STATE}.ravel"
        if [ -n "${STUB_OBJECTS_DOC_AT:-}" ]; then
          if [ -n "${STUB_OBJECTS_DOC:-}" ]; then
            printf '%s\n' "${STUB_OBJECTS_DOC}" >"${STUB_OBJECTS_DOC_AT}"
          else
            rm -f "${STUB_OBJECTS_DOC_AT}"
          fi
        fi
        ;;
      start)
        [ "${STUB_START_EXIT:-0}" -eq 0 ] || exit "${STUB_START_EXIT}"
        ;;
      run)
        case " $* " in
          *" list-objects-v2 "*)
            # The objects step's listing of Ravel's bucket: STUB_LISTING's
            # file, else STUB_LIST_DATA (default 300) data objects and one
            # key of every other kind; exit STUB_LIST_EXIT.
            [ "${STUB_LIST_EXIT:-0}" -eq 0 ] || exit "${STUB_LIST_EXIT}"
            if [ -n "${STUB_LISTING:-}" ]; then
              cat "${STUB_LISTING}"
            else
              python3 -I -c 'import json,sys; n=int(sys.argv[1]); print(json.dumps({"Contents": [{"Key": f"t/h1/m/l0/{i:06d}.rseg", "Size": 100} for i in range(n)] + [{"Key": "t/h1/m/c/a", "Size": 10}, {"Key": "sys/qualify/p", "Size": 1}, {"Key": "t/h1/catalog/x", "Size": 5}]}))' "${STUB_LIST_DATA:-300}"
            fi
            ;;
          # ravel-cli catalog fold on the ravel-qualify service: its text
          # report, the hours in the clock's current hour bucket. It logs
          # ravel-server's state as it runs.
          *" catalog fold "*)
            printf 'docker-state fold ravel-server=%s\n' "$(cat "${STUB_STATE}.ravel" 2>/dev/null || echo running)" >>"${STUB_LOG}"
            [ "${STUB_FOLD_EXIT:-0}" -eq 0 ] || exit "${STUB_FOLD_EXIT}"
            hour=$(($(date -u +%s) / 3600))
            printf 'store: s3\nsignal: metrics\nno_op: false\nrebuilt: false\nprevious_watermark_hour: None\n'
            printf 'watermark_hour: Some(%s)\nseal_margin: 1h 20m\n' "$((hour - ${STUB_FOLD_WATERMARK_LAG:-0}))"
            printf 'seal_through_hour: %s\nbuckets_folded: 2\nentry_count: 420\npart_bytes: 9000\n' \
              "${STUB_FOLD_SEAL:-Some(${hour})}"
            ;;
        esac
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
    if [[ "$ref" == *@* ]]; then
      printf '[{"Id":"sha256:imageid","RepoDigests":["%s@%s"]}]\n' "${ref%%[:@]*}" "${ref#*@}"
    else
      # A locally built image: no registry digest, only its content Id.
      printf '[{"Id":"%s","RepoDigests":[]}]\n' "${STUB_LOCAL_IMAGE_ID:-sha256:imageid}"
    fi
    ;;
  volume)
    [ -z "${STUB_LEFTOVER_VOLUME:-}" ] || echo "${STUB_LEFTOVER_VOLUME}"
    ;;
esac
exit 0
STUB

# curl: readiness 200, IMDS unreachable, Ravel's /metrics with or without the
# fold gauge, Mimir's /metrics (STUB_MIMIR_METRICS refused, 503 or empty makes
# it fail that way), Prometheus' refusal of the falsification query, and the s3s-fs
# release asset as stub bytes (STUB_S3S_BYTES replaces them).
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
  *:9009/metrics)
    case "${STUB_MIMIR_METRICS:-}" in
      refused) exit 7 ;;
      503) printf 'unavailable' >"$out"; printf 503 ;;
      empty) : >"$out"; printf 200 ;;
      *) printf 'thanos_objstore_bucket_operations_total{bucket="blocks",component="store-gateway",operation="get_range"} 25037\n' >"$out"; printf 200 ;;
    esac
    ;;
  */releases/download/s3s-fs-0.17.0-ravel.3/s3s-fs-linux-*)
    [ -z "${STUB_S3S_CURL_EXIT:-}" ] || exit "${STUB_S3S_CURL_EXIT}"
    printf '%s' "${STUB_S3S_BYTES:-stub-s3s-fs-${url##*-linux-}}" >"$out"
    ;;
  */metrics)
    # Scrape k (1-based) reports k*STUB_GETS_PER_SCRAPE (default 100) store
    # GETs, k*STUB_FETCH_MISSES_PER_SCRAPE (default 95) fetch-cache misses and
    # k*3 commit-record cache entries, so consecutive scrapes differ by those.
    # Every scrape reports metrics flushes by size 40, age 100, age_adaptive
    # 20, age_floor 30 and arrival 10 (200 disjoint), beside a logs flush
    # count and the rerouted and grace-extended sub-counts
    # (STUB_FLUSH_SUBCOUNT each, default 2000). STUB_NO_FETCH_MISSES and
    # STUB_NO_FLUSH_COUNTERS drop those families. From scrape
    # STUB_METRICS_DOWN_FROM on it answers 503.
    k=$(($(cat "${STUB_STATE}.scrapes" 2>/dev/null || echo 0) + 1))
    echo "$k" >"${STUB_STATE}.scrapes"
    if [ -n "${STUB_METRICS_DOWN_FROM:-}" ] && [ "$k" -ge "${STUB_METRICS_DOWN_FROM}" ]; then
      printf 'unavailable' >"$out"
      printf 503
      exit 0
    fi
    if [ -n "${STUB_NO_FOLD:-}" ]; then
      printf '# no fold yet\nravel_up 1\n' >"$out"
    else
      printf '# TYPE ravel_catalog_fold_last_success_timestamp_seconds gauge\nravel_catalog_fold_last_success_timestamp_seconds 1761000000\n' >"$out"
    fi
    printf 'ravel_store_calls_total{mode="all",op="get"} %s\nravel_store_calls_total{mode="all",op="put"} 7\n' "$((k * ${STUB_GETS_PER_SCRAPE:-100}))" >>"$out"
    if [ -z "${STUB_NO_FETCH_MISSES:-}" ]; then
      printf 'ravel_cache_misses_total{mode="all",cache="fetch"} %s\n' "$((k * ${STUB_FETCH_MISSES_PER_SCRAPE:-95}))" >>"$out"
    fi
    printf 'ravel_cache_misses_total{mode="all",cache="catalog"} %s\n' "$((k * 1000))" >>"$out"
    if [ -z "${STUB_NO_FLUSH_COUNTERS:-}" ]; then
      for pair in ${STUB_FLUSH_PAIRS:-size:40 age:100 age_adaptive:20 age_floor:30 arrival:10}; do
        printf 'ravel_ingest_flushes_by_%s_total{mode="all",signal="metrics"} %s\n' "${pair%%:*}" "${pair#*:}" >>"$out"
      done
    fi
    printf 'ravel_ingest_flushes_by_age_total{mode="all",signal="logs"} 5000\n' >>"$out"
    printf 'ravel_ingest_flushes_manual_total{mode="all",signal="metrics"} 0\n' >>"$out"
    printf 'ravel_ingest_rerouted_flushes_total{mode="all",signal="metrics",reason="retired_index"} %s\n' "${STUB_FLUSH_SUBCOUNT:-2000}" >>"$out"
    printf 'ravel_ingest_grace_extended_stale_flushes_total{mode="all",signal="metrics"} %s\n' "${STUB_FLUSH_SUBCOUNT:-2000}" >>"$out"
    printf 'ravel_catalog_record_cache_entries{mode="all",cache="commit"} %s\nravel_catalog_record_cache_entries{mode="all",cache="compaction"} 1\n' "$((k * 3))" >>"$out"
    printf 200
    ;;
  */api/v1/query)
    printf '{"status":"error","errorType":"execution","error":"query processing would load too many samples into memory in query execution"}' >"$out"
    printf 422
    ;;
  *) printf 200 ;;
esac
STUB

# uname: -m answers STUB_ARCH (default x86_64); anything else is the real one.
cat >"${BIN}/uname" <<'STUB'
#!/usr/bin/env bash
if [ "$*" = "-m" ]; then
  echo "${STUB_ARCH:-x86_64}"
else
  exec "${STUB_REAL_UNAME}" "$@"
fi
STUB

# sha256sum (in SHA256SUM_DIR) and shasum (in SHASUM_DIR, answering only
# -a 256): the pinned constant for the curl stub's bytes for that arch, the
# real sha256 for any other content. NOHASH_DIR links every other tool on
# PATH, so a case can run with either or neither hash tool.
SHA256SUM_DIR="${TMP}/hash-sha256sum"
SHASUM_DIR="${TMP}/hash-shasum"
NOHASH_DIR="${TMP}/nohash"
mkdir -p "${SHA256SUM_DIR}" "${SHASUM_DIR}" "${NOHASH_DIR}"
# write_hash_stub PATH NAME SHIFT: a hash stub whose file argument follows
# SHIFT leading arguments.
write_hash_stub() {
  cat >"$1" <<STUB
#!/usr/bin/env bash
printf '$2 %s\n' "\$*" >>"\${STUB_LOG}"
if [ "$3" -eq 2 ]; then
  [ "\$1 \$2" = "-a 256" ] || exit 2
  shift 2
fi
[ -f "\$1" ] || exit 1
case "\$(cat "\$1")" in
  stub-s3s-fs-x86_64) h=${S3S_SHA_X86_64} ;;
  stub-s3s-fs-aarch64) h=${S3S_SHA_AARCH64} ;;
  *) h="\$(python3 -I -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "\$1")" ;;
esac
printf '%s  %s\n' "\$h" "\$1"
STUB
  chmod +x "$1"
}
write_hash_stub "${SHA256SUM_DIR}/sha256sum" sha256sum 0
write_hash_stub "${SHASUM_DIR}/shasum" shasum 2
IFS=: read -ra path_dirs <<<"${PATH}"
for d in "${path_dirs[@]}"; do
  [ -d "$d" ] || continue
  for t in "$d"/*; do
    n="${t##*/}"
    case "$n" in sha256sum | shasum) continue ;; esac
    [ -x "$t" ] && [ ! -e "${NOHASH_DIR}/${n}" ] && ln -s "$t" "${NOHASH_DIR}/${n}"
  done
done

# metricsbench_ingest: an ingest report with a row per endpoint flag given.
cat >"${BIN}/metricsbench_ingest" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_ingest %s\n' "$*" >>"${STUB_LOG}"
printf 'metricsbench_ingest env token=%s\n' "${METRICSBENCH_RAVEL_TOKEN:+set}" >>"${STUB_LOG}"
# --readback-only: one row's read-back against an existing ingest report;
# STUB_READBACK_ONLY_EXIT 1 is a read error, as the bin reports one.
if [ "${1:-}" = "--readback-only" ]; then
  case "${STUB_READBACK_ONLY_EXIT:-0}" in
    0) echo '{"system":"ravel","plan_series_count":1000,"readback":{"series_count":1000,"sample_count":120000,"matches_expected":true,"mismatches":[],"query_ms":{"series":12,"samples.0":5}}}' ;;
    1) echo '{"system":"ravel","plan_series_count":1000,"readback_error":"query series failed: 503"}' ;;
  esac
  exit "${STUB_READBACK_ONLY_EXIT:-0}"
fi
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
      if [ "${STUB_INGEST_ROW_FAILURE:-}" = "$sys" ]; then
        rows="${rows}${rows:+,}{\"system\":\"${sys}\",\"row_failure\":{\"band\":null,\"error\":\"skipped after an earlier failure\"}}"
      elif [ "${STUB_INGEST_READBACK_FAIL:-}" = "$sys" ]; then
        rows="${rows}${rows:+,}{\"system\":\"${sys}\",\"readback_error\":\"query samples.0 failed: 422\"}"
      else
        rows="${rows}${rows:+,}{\"system\":\"${sys}\",\"readback\":{\"series_count\":1000,\"sample_count\":120000,\"matches_expected\":true,\"mismatches\":[]}}"
      fi
      ;;
  esac
done
stalled=""
[ -z "${STUB_INGEST_STALL:-}" ] ||
  stalled=",\"stalled\":[{\"system\":\"${STUB_INGEST_STALL}\",\"error\":\"projected 9000 s past the 600 s ceiling\"}]"
printf '{"profile":{"name":"ci","scrape_interval_secs":15,"run":{"steps":120,"anchor_ms":%s}},"systems":[%s]%s}\n' "$anchor" "$rows" "$stalled"
if [ -n "${STUB_INGEST_READBACK_FAIL:-}${STUB_INGEST_STALL:-}${STUB_INGEST_EXIT_AFTER_REPORT:-}" ]; then
  echo "stub ingest: a row failed its verification" >&2
  exit 1
fi
STUB

cat >"${BIN}/metricsbench_query" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_query %s\n' "$*" >>"${STUB_LOG}"
printf 'metricsbench_query env token=%s\n' "${METRICSBENCH_RAVEL_TOKEN:+set}" >>"${STUB_LOG}"
args=("$@")
verdict=""
for a in "$@"; do
  [ "$a" = "--diagnostic-after-failed-ingest" ] && verdict=',"run_verdict":"failed"'
done
# As the real bin does: a diagnostic whose failed row is the Prometheus
# oracle's is refused, and nothing is written.
if [ -n "$verdict" ] && [ "${STUB_INGEST_READBACK_FAIL:-}" = prometheus ]; then
  echo "stub query: the oracle prometheus failed its read-back" >&2
  exit 65
fi
for ((i = 0; i < ${#args[@]}; i++)); do
  [ "${args[$i]}" = "--out" ] && printf '{"stub":"report"%s}\n' "$verdict" >"${args[$((i + 1))]}"
done
exit 0
STUB

cat >"${BIN}/metricsbench_report" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_report %s\n' "$*" >>"${STUB_LOG}"
case "$1" in
  provenance) printf '{"ravel_git_commit":"%s"}\n' "${GITHUB_SHA}" ;;
  validate)
    [ -s "$2" ] || exit 1
    if grep -qF run_verdict "$2"; then
      echo "report $2 does not stand: report is a diagnostic of a failed run, never a result" >&2
      exit 1
    fi
    ;;
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
# HASH_TOOLS picks the hash stubs on PATH: sha256sum (default), shasum, or
# none; the latter two drop the host's own from PATH as well.
run_case() {
  local name="$1"
  shift
  OUT="${TMP}/${name}/out"
  LOG="${TMP}/${name}/calls.log"
  ERR="${TMP}/${name}/stderr.log"
  mkdir -p "${TMP}/${name}"
  : >"${LOG}"
  rc=0
  local run_path
  case "${HASH_TOOLS:-sha256sum}" in
    sha256sum) run_path="${BIN}:${SHA256SUM_DIR}:${PATH}" ;;
    shasum) run_path="${BIN}:${SHASUM_DIR}:${NOHASH_DIR}" ;;
    none) run_path="${BIN}:${NOHASH_DIR}" ;;
  esac
  PATH="${run_path}" \
    STUB_LOG="${LOG}" STUB_STATE="${TMP}/${name}/state" STUB_REAL_UNAME="${REAL_UNAME}" \
    METRICSBENCH_S3S_FS_CACHE="${TMP}/${name}/cache" \
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

# --- a: success --------------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/a/out")
run_case a "${args[@]}" --rss-band ravel-server:1:1000000000
expect_rc a 0
for f in stamps.json ingest.json effective-config.json provenance.json report.json memory-summary.json memory.jsonl compose.log; do
  expect_file a "$f"
done
check "a: stamps status passed, exit 0" 'j("stamps.json")["status"] == "passed" and j("stamps.json")["exit_code"] == 0'
check "a: every step recorded in order" 'j("stamps.json")["steps_completed"] == ["fetch-s3s-fs","up","qualify","image","ready","sampler","ingest","mimir-readiness","objects","capture","provenance","fold-state","query","validate","memory","stop","fold","start","readback-folded"]'
check "a: published image kind stamped, no image_id" 'j("stamps.json")["ravel_image"]["kind"] == "published" and "image_id" not in j("stamps.json")["ravel_image"]'
expect_log "a: provenance says the image is published" "--config ravel_image_kind=published"
expect_no_log "a: provenance carries no local image id" "ravel_image_id="
check "a: image reference and resolved digest stamped" 'j("stamps.json")["ravel_image"]["reference"] == "'"${IMAGE}"'" and j("stamps.json")["ravel_image"]["resolved"]["digest_matches_reference"] is True'
check "a: source commit, store kind, profile, anchor stamped" 'j("stamps.json")["ravel_source_commit"] == "'"${COMMIT}"'" and j("stamps.json")["store_kind"] == "s3s-fs" and j("stamps.json")["profile"] == "ci" and j("stamps.json")["anchor_ms"] == '"${ANCHOR}"' and j("stamps.json")["anchor_source"] == "flag"'
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
A_BIN="${TMP}/a/cache/${S3S_RELEASE}/s3s-fs-linux-x86_64"
expect_log "a: the x86_64 asset downloaded" "https://github.com/NOFireAI/s3s/releases/download/${S3S_RELEASE}/s3s-fs-linux-x86_64"
check "a: object_store stamped from the download" 'j("stamps.json")["object_store"] == {"implementation": "s3s-fs", "release": "'"${S3S_RELEASE}"'", "arch": "x86_64", "engine_arch": "amd64", "engine_arch_source": "docker version (Server.Arch)", "sha256": "'"${S3S_SHA_X86_64}"'", "url": "https://github.com/NOFireAI/s3s/releases/download/'"${S3S_RELEASE}"'/s3s-fs-linux-x86_64", "path": "'"${A_BIN}"'", "source": "download"}'
if [ -x "${A_BIN}" ] && [ "$(cat "${A_BIN}")" = "stub-s3s-fs-x86_64" ]; then
  pass "a: the verified download is cached executable"
else
  fail "a: no executable verified binary at ${A_BIN}"
fi
expect_log "a: compose up gets the binary path" "docker-env up S3S_FS_BIN=${A_BIN}"
expect_log "a: provenance records the store" "--store-backend s3s-fs --region us-east-1 --endpoint http://s3s:9000"
expect_log "a: provenance records the store's release" "--config store_release=${S3S_RELEASE}"
expect_log "a: provenance records the store's sha256" "--config store_sha256=${S3S_SHA_X86_64}"
expect_no_log "a: query is not run as a diagnostic" "--diagnostic-after-failed-ingest"
check "a: nothing failed after ingest" 'j("stamps.json")["failed_after_ingest"] == []'

# --- b: ingest fails ---------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/b/out")
STUB_INGEST_EXIT=1 run_case b "${args[@]}"
expect_rc b 70
expect_err "b: message names step ingest" "step ingest failed"
check "b: stamps name step ingest, status failed" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["status"] == "failed" and j("stamps.json")["exit_code"] == 70'
expect_no_file b report.json
expect_no_log "b: no query run" "metricsbench_query"
expect_log "b: still torn down" " down -v"
expect_file b memory-summary.json

# --- q: a row's read-back fails ------------------------------------------------
mapfile -t args < <(full_args "${TMP}/q/out")
STUB_INGEST_READBACK_FAIL=mimir run_case q "${args[@]}"
expect_rc q 70
expect_err "q: message names step ingest and the row" "step ingest failed: metricsbench_ingest exited 1: mimir: read-back failed"
expect_err "q: message says the run went on as a diagnostic" "the run went on as a diagnostic"
check "q: stamps name step ingest, status failed, exit 70" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["status"] == "failed" and j("stamps.json")["exit_code"] == 70'
check "q: every later step ran, ingest and mimir-readiness not done" 'j("stamps.json")["steps_completed"] == ["fetch-s3s-fs","up","qualify","image","ready","sampler","objects","capture","provenance","fold-state","query","validate","memory","stop","fold","start","readback-folded"]'
check "q: nothing failed after ingest" 'j("stamps.json")["failed_after_ingest"] == []'
check "q: Mimir readiness line stamped as not matching" 'j("stamps.json")["mimir_readiness"].startswith("ingest read-back for mimir did not match")'
expect_log "q: query run as a diagnostic" "--diagnostic-after-failed-ingest"
check "q: the report says run_verdict failed" 'j("report.json")["run_verdict"] == "failed"'
expect_log "q: report validated by path" "metricsbench_report validate ${OUT}/report.json"
expect_err "q: validate refused the diagnostic" "step validate refused the diagnostic report"
expect_file q memory-summary.json
expect_log "q: torn down" " down -v"

# --- q2: a row stalls --------------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/q2/out")
STUB_INGEST_STALL=victoriametrics run_case q2 "${args[@]}"
expect_rc q2 70
expect_err "q2: message names the stalled row" "victoriametrics: stalled"
check "q2: stamps name step ingest" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["exit_code"] == 70'
expect_log "q2: query run as a diagnostic" "--diagnostic-after-failed-ingest"
check "q2: Mimir readiness still checked" '"mimir-readiness" in j("stamps.json")["steps_completed"]'

# --- q3: a failed read-back and an --rss-band breach ------------------------------
mapfile -t args < <(full_args "${TMP}/q3/out")
STUB_INGEST_READBACK_FAIL=victoriametrics STUB_MEM_ravel_server=2GiB run_case q3 "${args[@]}" \
  --rss-band ravel-server:0:1073741824
expect_rc q3 70
check "q3: stamps name ingest and list memory as failed after it" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["failed_after_ingest"] == ["memory"] and j("stamps.json")["deferred_failures"] == ["memory"]'
expect_err "q3: the band is still said" "--rss-band ravel-server:0:1073741824 above"
check "q3: the breach does not stop the folded steps, memory not done" 'j("stamps.json")["steps_completed"][-5:] == ["validate","stop","fold","start","readback-folded"] and "memory" not in j("stamps.json")["steps_completed"]'
expect_log "q3: the fold ran after the breach" "catalog fold"
expect_log "q3: the folded read-back ran after the breach" "--readback-only"
expect_err "q3: the exit still names ingest" "step ingest failed:"

# --- q4: the oracle's read-back fails ----------------------------------------------
# query refuses a diagnostic whose failed row is Prometheus's, so the run ends at
# query: no report, no validate, no memory step. The --rss-band breach is still
# checked on the way out and recorded in memory-summary.json, but it is not
# listed in failed_after_ingest.
mapfile -t args < <(full_args "${TMP}/q4/out")
STUB_INGEST_READBACK_FAIL=prometheus STUB_MEM_ravel_server=2GiB run_case q4 "${args[@]}" \
  --rss-band ravel-server:0:1073741824
expect_rc q4 70
check "q4: stamps name ingest and list only query as failed after it" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["failed_after_ingest"] == ["query"]'
check "q4: query, validate and memory not done" 'not {"query","validate","memory"} & set(j("stamps.json")["steps_completed"]) and "fold-state" in j("stamps.json")["steps_completed"]'
expect_log "q4: query run as a diagnostic" "--diagnostic-after-failed-ingest"
expect_err "q4: message names step query" "step query failed: metricsbench_query exited 65"
expect_err "q4: message says no report was written" "the run went on as a diagnostic and wrote no report.json"
expect_no_file q4 report.json
expect_no_log "q4: validate not run" "metricsbench_report validate"
expect_file q4 memory-summary.json
check "q4: the breach is recorded in memory-summary.json" 'j("memory-summary.json")["verdict"] == "band_violated" and [b["verdict"] for b in j("memory-summary.json")["bands"]] == ["above"]'
expect_log "q4: torn down" " down -v"
expect_no_log "q4: the run ends before the fold" "catalog fold"

# --- r: ingest fails with every row verified ---------------------------------------
mapfile -t args < <(full_args "${TMP}/r/out")
STUB_INGEST_EXIT_AFTER_REPORT=1 run_case r "${args[@]}"
expect_rc r 70
check "r: stamps name step ingest" 'j("stamps.json")["failed_step"] == "ingest"'
expect_no_log "r: no query run" "metricsbench_query"
expect_no_log "r: capture not run" "capture --out"

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
check "d: memory listed in deferred_failures" 'j("stamps.json")["deferred_failures"] == ["memory"] and j("stamps.json")["failed_after_ingest"] == []'
check "d: stop, fold, start and readback-folded ran after validate, memory not done" 'j("stamps.json")["steps_completed"][-5:] == ["validate","stop","fold","start","readback-folded"] and "memory" not in j("stamps.json")["steps_completed"]'
check "d: the folded read-back stamped" 'j("stamps.json")["readback_folded"]["exit_code"] == 0 and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["precondition"] == "met"'
expect_file d readback-folded.json
expect_err "d: message says the folded steps run first" "going on to the folded steps first"
expect_err "d: closing message names memory after the folded steps" "the folded steps ran after it"
expect_log "d: torn down" " down -v"

# --- e: OOM kill -------------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/e/out")
STUB_OOM_SERVICE=mimir run_case e "${args[@]}"
expect_rc e 75
expect_err "e: message names the container" "metricsbench-mimir-1 was OOM-killed"
check "e: summary names the OOM-killed container" 'j("memory-summary.json")["oom_killed"] == ["metricsbench-mimir-1"] and j("memory-summary.json")["containers"]["metricsbench-mimir-1"]["oom_killed"] is True and j("memory-summary.json")["verdict"] == "oom_killed"'
check "e: stamps name step memory, memory in deferred_failures" 'j("stamps.json")["failed_step"] == "memory" and j("stamps.json")["exit_code"] == 75 and j("stamps.json")["deferred_failures"] == ["memory"]'
check "e: stop, fold, start and readback-folded ran after the OOM kill" 'j("stamps.json")["steps_completed"][-4:] == ["stop","fold","start","readback-folded"] and "memory" not in j("stamps.json")["steps_completed"]'
expect_log "e: the folded read-back ran" "--readback-only"

# --- e2: a memory failure, then a failed folded read-back -------------------------
mapfile -t args < <(full_args "${TMP}/e2/out")
STUB_MEM_ravel_server=2GiB STUB_READBACK_ONLY_EXIT=1 run_case e2 "${args[@]}" --rss-band ravel-server:0:1073741824
expect_rc e2 70
check "e2: stamps name step readback-folded, memory in deferred_failures" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["exit_code"] == 70 and j("stamps.json")["deferred_failures"] == ["memory"]'
expect_err "e2: message names both failures" "step readback-folded failed: metricsbench_ingest --readback-only exited 1: query series failed: 503; step memory had failed before it: "
expect_err "e2: the band named in the same message" "step memory had failed before it: --rss-band ravel-server:0:1073741824 above"

# --- e3: the diagnostic mode still ends at a memory failure at once ---------------
run_case e3 --profile history --bin-dir "${BIN}" --out-dir "${TMP}/e3/out" --anchor-ms "${ANCHOR}" \
  --diagnostic-prometheus-max-samples 1000 --rss-band prometheus:0:1
expect_rc e3 75
check "e3: stamps name step memory, nothing deferred" 'j("stamps.json")["failed_step"] == "memory" and j("stamps.json")["deferred_failures"] == [] and j("stamps.json")["steps_completed"][-1] == "falsification"'
expect_err "e3: message names the band" "--rss-band prometheus:0:1 above"
expect_no_log "e3: no folded step" "--readback-only"
if grep -qF "going on to the folded steps" "${ERR}"; then fail "e3: diagnostic deferred the memory failure"; else pass "e3: diagnostic did not defer the memory failure"; fi

# --- e4: a memory failure when the folded steps are skipped -----------------------
mapfile -t args < <(full_args "${TMP}/e4/out")
STUB_INGEST_READBACK_FAIL=victoriametrics STUB_INGEST_ROW_FAILURE=ravel STUB_MEM_ravel_server=2GiB run_case e4 "${args[@]}" \
  --rss-band ravel-server:0:1073741824
expect_rc e4 70
check "e4: stamps name ingest, memory in failed_after_ingest and deferred_failures, the four skipped" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["failed_after_ingest"] == ["memory"] and j("stamps.json")["deferred_failures"] == ["memory"] and not {"stop","fold","start","readback-folded"} & set(j("stamps.json")["steps_completed"]) and j("stamps.json")["readback_folded"].startswith("skipped: the ravel row has a row_failure")'

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
expect_no_log "f: no s3s-fs download" "s3s-fs-linux"
check "f: stamps say the object store does not apply" 'j("stamps.json")["object_store"].startswith("not applicable")'
check "f: no objects step, stamped not applicable" '"objects" not in j("stamps.json")["steps_completed"] and j("stamps.json")["ravel_objects"].startswith("not applicable")'
expect_no_log "f: Ravel's bucket not listed" "list-objects-v2"
expect_no_file f ravel-objects.json

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
STUB_LEFTOVER_VOLUME=metricsbench_s3s-data run_case h5 "${args[@]}"
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

# --- n: s3s-fs fetch ------------------------------------------------------------
# seed_cache CASE ARCH BYTES: a cached s3s-fs binary for CASE's run.
seed_cache() {
  mkdir -p "${TMP}/$1/cache/${S3S_RELEASE}"
  printf '%s' "$3" >"${TMP}/$1/cache/${S3S_RELEASE}/s3s-fs-linux-$2"
  chmod 0755 "${TMP}/$1/cache/${S3S_RELEASE}/s3s-fs-linux-$2"
}

N1_BIN="${TMP}/n1/cache/${S3S_RELEASE}/s3s-fs-linux-x86_64"
seed_cache n1 x86_64 stub-s3s-fs-x86_64
mapfile -t args < <(full_args "${TMP}/n1/out")
run_case n1 "${args[@]}"
expect_rc "n1 cached match" 0
expect_no_log "n1 cached match: no download" "releases/download"
expect_log "n1 cached match: the cached binary was hashed" "sha256sum ${N1_BIN}"
check "n1 cached match: stamped from the cache with the pinned sha256" 'j("stamps.json")["object_store"]["source"] == "cache" and j("stamps.json")["object_store"]["sha256"] == "'"${S3S_SHA_X86_64}"'" and j("stamps.json")["object_store"]["path"] == "'"${N1_BIN}"'"'
expect_log "n1 cached match: compose up gets the cached path" "docker-env up S3S_FS_BIN=${N1_BIN}"

mapfile -t args < <(full_args "${TMP}/n2/out")
STUB_S3S_BYTES=tampered run_case n2 "${args[@]}"
expect_rc "n2 download mismatch" 65
expect_err "n2 download mismatch: refusal names the pinned sha256" "not the pinned ${S3S_SHA_X86_64}"
check "n2 download mismatch: stamps name step fetch-s3s-fs, exit 65" 'j("stamps.json")["failed_step"] == "fetch-s3s-fs" and j("stamps.json")["exit_code"] == 65 and j("stamps.json")["steps_completed"] == [] and j("stamps.json")["object_store"] is None'
expect_no_log "n2 download mismatch: nothing started" " up -d"
if [ -z "$(ls -A "${TMP}/n2/cache/${S3S_RELEASE}" 2>/dev/null)" ]; then
  pass "n2 download mismatch: nothing cached"
else
  fail "n2 download mismatch: the cache holds $(ls -A "${TMP}/n2/cache/${S3S_RELEASE}")"
fi

seed_cache n3 x86_64 tampered
mapfile -t args < <(full_args "${TMP}/n3/out")
run_case n3 "${args[@]}"
expect_rc "n3 cached mismatch" 65
expect_err "n3 cached mismatch: refusal names the cached file" "the cached ${TMP}/n3/cache/${S3S_RELEASE}/s3s-fs-linux-x86_64 has sha256"
check "n3 cached mismatch: stamps name step fetch-s3s-fs" 'j("stamps.json")["failed_step"] == "fetch-s3s-fs" and j("stamps.json")["exit_code"] == 65'
expect_no_log "n3 cached mismatch: no download" "releases/download"
expect_no_log "n3 cached mismatch: nothing started" " up -d"

mapfile -t args < <(full_args "${TMP}/n4/out")
STUB_ENGINE_ARCH=s390x run_case n4 "${args[@]}"
expect_rc "n4 unknown arch" 65
expect_err "n4 unknown arch: refusal names the engine arch and its source" "no s3s-fs release asset is pinned for engine arch 's390x' (from docker version (Server.Arch))"
expect_no_log "n4 unknown arch: no download" "releases/download"
expect_no_log "n4 unknown arch: nothing started" "compose -f"
expect_no_file n4 stamps.json

mapfile -t args < <(full_args "${TMP}/n5/out")
STUB_ENGINE_DOWN=1 STUB_ARCH=aarch64 run_case n5 "${args[@]}"
expect_rc "n5 aarch64" 0
expect_log "n5 aarch64: the aarch64 asset downloaded" "/releases/download/${S3S_RELEASE}/s3s-fs-linux-aarch64"
check "n5 aarch64: stamped with the aarch64 sha256" 'j("stamps.json")["object_store"]["arch"] == "aarch64" and j("stamps.json")["object_store"]["sha256"] == "'"${S3S_SHA_AARCH64}"'" and j("stamps.json")["host"]["arch"] == "aarch64"'
check "n5 aarch64: stamped as read from uname -m" 'j("stamps.json")["object_store"]["engine_arch"] == "aarch64" and j("stamps.json")["object_store"]["engine_arch_source"].startswith("uname -m")'
expect_log "n5 aarch64: provenance records the aarch64 sha256" "--config store_sha256=${S3S_SHA_AARCH64}"

mapfile -t args < <(full_args "${TMP}/n6/out")
STUB_S3S_CURL_EXIT=22 run_case n6 "${args[@]}"
expect_rc "n6 download fails" 70
check "n6 download fails: stamps name step fetch-s3s-fs" 'j("stamps.json")["failed_step"] == "fetch-s3s-fs" and j("stamps.json")["exit_code"] == 70'
expect_no_log "n6 download fails: nothing started" " up -d"

mapfile -t args < <(full_args "${TMP}/n7/out")
STUB_ENGINE_ARCH=arm64 STUB_ARCH=arm64 run_case n7 "${args[@]}"
expect_rc "n7 arm64 engine, arm64 host" 0
expect_log "n7 arm64 engine: the aarch64 asset downloaded" "/releases/download/${S3S_RELEASE}/s3s-fs-linux-aarch64"
check "n7 arm64 engine: stamped aarch64 from docker version" 'j("stamps.json")["object_store"]["arch"] == "aarch64" and j("stamps.json")["object_store"]["sha256"] == "'"${S3S_SHA_AARCH64}"'" and j("stamps.json")["object_store"]["engine_arch"] == "arm64" and j("stamps.json")["object_store"]["engine_arch_source"] == "docker version (Server.Arch)" and j("stamps.json")["host"]["arch"] == "arm64"'

mapfile -t args < <(full_args "${TMP}/n8/out")
STUB_ENGINE_ARCH=amd64 STUB_ARCH=aarch64 run_case n8 "${args[@]}"
expect_rc "n8 amd64 engine, aarch64 host" 0
expect_log "n8 amd64 engine: the x86_64 asset downloaded" "/releases/download/${S3S_RELEASE}/s3s-fs-linux-x86_64"
expect_no_log "n8 amd64 engine: no aarch64 asset" "s3s-fs-linux-aarch64"
check "n8 amd64 engine: stamped x86_64 with its sha256" 'j("stamps.json")["object_store"]["arch"] == "x86_64" and j("stamps.json")["object_store"]["sha256"] == "'"${S3S_SHA_X86_64}"'" and j("stamps.json")["object_store"]["engine_arch"] == "amd64"'

mapfile -t args < <(full_args "${TMP}/n9/out")
HASH_TOOLS=shasum run_case n9 "${args[@]}"
expect_rc "n9 shasum only" 0
expect_log "n9 shasum only: the download hashed with shasum -a 256" "shasum -a 256 ${TMP}/n9/cache/${S3S_RELEASE}/s3s-fs-linux-x86_64.download."
check "n9 shasum only: stamped with the pinned sha256" 'j("stamps.json")["object_store"]["sha256"] == "'"${S3S_SHA_X86_64}"'"'

mapfile -t args < <(full_args "${TMP}/n10/out")
HASH_TOOLS=shasum STUB_S3S_BYTES=tampered run_case n10 "${args[@]}"
expect_rc "n10 shasum only, download mismatch" 65
expect_log "n10 shasum only: the download hashed with shasum -a 256" "shasum -a 256 "
expect_err "n10 shasum only: refusal names the pinned sha256" "not the pinned ${S3S_SHA_X86_64}"
expect_no_log "n10 shasum only: nothing started" " up -d"

mapfile -t args < <(full_args "${TMP}/n11/out")
HASH_TOOLS=none run_case n11 "${args[@]}"
expect_rc "n11 no hash tool" 65
expect_err "n11 no hash tool: refusal names both tools" "neither sha256sum nor shasum is on PATH"
expect_no_log "n11 no hash tool: no download" "releases/download"
expect_no_log "n11 no hash tool: nothing started" "compose -f"
expect_no_file n11 stamps.json

# --- p: local-build image ------------------------------------------------------
LOCAL_REF="ravel-server:nightly-${COMMIT}"
LOCAL_ID="sha256:$(printf 'b%.0s' $(seq 64))"
OTHER_ID="sha256:$(printf 'c%.0s' $(seq 64))"
local_args() {
  printf '%s\n' --profile "${2:-ci}" --ravel-image "${LOCAL_REF}" --ravel-image-id "${LOCAL_ID}" \
    --ravel-source-commit "${COMMIT}" --bin-dir "${BIN}" --out-dir "$1" --anchor-ms "${ANCHOR}"
}
mapfile -t args < <(local_args "${TMP}/p1/out")
STUB_LOCAL_IMAGE_ID="${LOCAL_ID}" run_case p1 "${args[@]}"
expect_rc "p local build" 0
check "p local build: stamps say local-build with the Id, reference and source commit" 'j("stamps.json")["ravel_image"]["kind"] == "local-build" and j("stamps.json")["ravel_image"]["image_id"] == "'"${LOCAL_ID}"'" and j("stamps.json")["ravel_image"]["reference"] == "'"${LOCAL_REF}"'" and j("stamps.json")["ravel_source_commit"] == "'"${COMMIT}"'"'
check "p local build: image step resolved the same Id, no registry digest" 'j("stamps.json")["ravel_image"]["resolved"]["id_matches"] is True and j("stamps.json")["ravel_image"]["resolved"]["image_id"] == "'"${LOCAL_ID}"'" and j("stamps.json")["ravel_image"]["resolved"]["repo_digests"] == [] and "image" in j("stamps.json")["steps_completed"]'
expect_log "p local build: provenance says local-build" "--config ravel_image_kind=local-build"
expect_log "p local build: provenance carries the Id" "--config ravel_image_id=${LOCAL_ID}"
expect_log "p local build: compose brought up" "ravel.yml up -d"

mapfile -t args < <(local_args "${TMP}/p2/out")
STUB_LOCAL_IMAGE_ID="${OTHER_ID}" run_case p2 "${args[@]}"
expect_rc "p Id differs" 65
expect_err "p Id differs: refusal names the Id" "does not report Id ${LOCAL_ID}"
expect_no_log "p Id differs: nothing started" " up -d"
expect_no_file p2 stamps.json

mapfile -t args < <(local_args "${TMP}/p3/out" history)
STUB_LOCAL_IMAGE_ID="${LOCAL_ID}" run_case p3 "${args[@]}"
expect_rc "p local build on history" 65
expect_err "p local build on history: refusal says nightly only" "a local build is for the nightly only"
expect_no_log "p local build on history: nothing started" "compose -f"
expect_no_file p3 stamps.json

run_case p4 --profile ci --ravel-image "${IMAGE}" --ravel-image-id "${LOCAL_ID}" \
  --ravel-source-commit "${COMMIT}" --bin-dir "${BIN}" --out-dir "${TMP}/p4/out" --anchor-ms "${ANCHOR}"
expect_rc "p both forms" 64
expect_err "p both forms: message says not both" "not both"
expect_no_log "p both forms: nothing started" "compose -f"

run_case p5 --profile ci --ravel-source-commit "${COMMIT}" --bin-dir "${BIN}" \
  --out-dir "${TMP}/p5/out" --anchor-ms "${ANCHOR}"
expect_rc "p neither form" 64
expect_err "p neither form: message says an image is required" "a Ravel image is required"
expect_no_log "p neither form: nothing started" "compose -f"

run_case p6 --profile ci --ravel-image-id "${LOCAL_ID}" --ravel-source-commit "${COMMIT}" \
  --bin-dir "${BIN}" --out-dir "${TMP}/p6/out" --anchor-ms "${ANCHOR}"
expect_rc "p id without --ravel-image" 64
expect_no_log "p id without --ravel-image: nothing started" "compose -f"

# --- o: descriptor limits --------------------------------------------------------
# A service's block runs from its two-space key to the next key at two spaces
# or less; the four ulimits lines must appear in it, in order.
for pair in docker-compose.yml:prometheus docker-compose.yml:victoriametrics \
  docker-compose.yml:s3s docker-compose.yml:mimir ravel.yml:ravel-server; do
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

# --- s: the real-S3 lane --------------------------------------------------------
mapfile -t args < <(full_args "${TMP}/s/out")
run_case s "${args[@]}" --real-s3-bucket-prefix mb-run1 --real-s3-region us-east-1
expect_rc s 0
expect_log "s: compose takes the real-S3 overlay" "real-s3.yml"
expect_log "s: compose up gets the prefix and region" "docker-env up S3_PREFIX=mb-run1 S3_REGION=us-east-1"
expect_log "s: provenance records real S3, billing requests" "--store-backend s3 --region us-east-1 --endpoint n/a --bills-requests"
expect_no_log "s: provenance does not name s3s-fs" "--store-backend s3s-fs"
expect_no_log "s: provenance carries no s3s-fs release" "store_release="
expect_no_log "s: nothing downloads s3s-fs" "s3s-fs-linux"
check "s: stamps name real S3 and every bucket" 'j("stamps.json")["store_kind"] == "s3" and j("stamps.json")["object_store"] == {"implementation": "s3", "region": "us-east-1", "credentials": "instance-role", "buckets": ["mb-run1-ravel", "mb-run1-mimir-blocks", "mb-run1-mimir-ruler", "mb-run1-mimir-alertmanager"]}'
check "s: fetch-s3s-fs did not run" '"fetch-s3s-fs" not in j("stamps.json")["steps_completed"] and "query" in j("stamps.json")["steps_completed"]'

# An engine arch with no s3s-fs asset does not matter when no asset is fetched.
mapfile -t args < <(full_args "${TMP}/s8/out")
STUB_ENGINE_ARCH=riscv64 run_case s8 "${args[@]}" --real-s3-bucket-prefix mb-run1 --real-s3-region us-east-1
expect_rc s8 0

# The default lane stays on s3s-fs and never reads the overlay.
mapfile -t args < <(full_args "${TMP}/s1/out")
run_case s1 "${args[@]}"
expect_rc s1 0
expect_no_log "s1: no real-S3 overlay by default" "real-s3.yml"
expect_no_log "s1: no billing flag by default" "--bills-requests"
expect_log "s1: no bucket prefix by default" "docker-env up S3_PREFIX=<unset> S3_REGION=<unset>"
check "s1: stamps say s3s-fs" 'j("stamps.json")["store_kind"] == "s3s-fs" and j("stamps.json")["object_store"]["implementation"] == "s3s-fs"'

mapfile -t args < <(full_args "${TMP}/s2/out")
run_case s2 "${args[@]}" --real-s3-bucket-prefix mb-run1
expect_rc s2 64
expect_err "s2: a prefix needs a region" "go together"
expect_err "s2: the usage it prints names both flags" "--real-s3-bucket-prefix PREFIX --real-s3-region REGION"

mapfile -t args < <(full_args "${TMP}/s3/out")
run_case s3 "${args[@]}" --real-s3-region us-east-1
expect_rc s3 64
expect_err "s3: a region needs a prefix" "go together"

mapfile -t args < <(full_args "${TMP}/s4/out")
run_case s4 "${args[@]}" --real-s3-bucket-prefix MB_Run --real-s3-region us-east-1
expect_rc s4 64
expect_err "s4: a prefix S3 cannot name is refused" "--real-s3-bucket-prefix takes"

mapfile -t args < <(full_args "${TMP}/s5/out")
run_case s5 "${args[@]}" --real-s3-bucket-prefix "$(printf 'a%.0s' {1..45})" --real-s3-region us-east-1
expect_rc s5 64
expect_err "s5: a prefix past 44 characters is refused" "at most 44 characters"

mapfile -t args < <(full_args "${TMP}/s6/out")
run_case s6 "${args[@]}" --real-s3-bucket-prefix mb-run1 --real-s3-region useast1
expect_rc s6 64
expect_err "s6: a malformed region is refused" "--real-s3-region takes"

run_case s7 --profile history --bin-dir "${BIN}" --out-dir "${TMP}/s7/out" --diagnostic-prometheus-max-samples 100000 \
  --real-s3-bucket-prefix mb-run1 --real-s3-region us-east-1
expect_rc s7 64
expect_err "s7: the falsification run takes no real-S3 flags" "takes no --real-s3 flags"

# --- t: real S3 with a local build -----------------------------------------------
STUB_LOCAL_IMAGE_ID="${LOCAL_ID}" run_case t --profile ci --ravel-image "${LOCAL_REF}" --ravel-image-id "${LOCAL_ID}" \
  --ravel-source-commit "${COMMIT}" --bin-dir "${BIN}" --out-dir "${TMP}/t/out" --anchor-ms "${ANCHOR}" \
  --real-s3-bucket-prefix mb-run1 --real-s3-region us-east-1
expect_rc t 65
expect_err "t: a local build is refused in the real-S3 lane" "a local build is never publishable"
expect_no_log "t: nothing started" "docker-env up"

# --- u: stop, fold, start and the folded read-back -------------------------------
# u reads case a's output; a's step list above pins stop, fold, start and
# readback-folded after memory, in that order.
OUT="${TMP}/a/out"
LOG="${TMP}/a/calls.log"
for f in fold.out readback-folded.json ravel-metrics-before-folded-readback.txt ravel-metrics-after-folded-readback.txt; do
  expect_file u "$f"
done
expect_log "u: the fold runs ravel-cli on the ravel-qualify service with the server's flags" "ravel.yml run --rm --no-deps -T ravel-qualify --store s3 --tenant-hash-unkeyed catalog fold --tenant metricsbench --shards 4 --signal metrics --writers-stopped"
if grep -A1 -xF '      - "--shards"' "${HERE}/../ravel.yml" | grep -qxF '      - "4"'; then
  pass "u: ravel.yml's server runs --shards 4, the count the fold names"
else
  fail "u: ravel.yml's ravel-server does not run --shards 4"
fi
if [ "$(grep -cE '^docker compose .* stop ravel-server$' "${LOG}")" -eq 1 ] &&
  [ "$(grep -cE '^docker compose .* (stop|start|restart) ' "${LOG}")" -eq 1 ]; then
  pass "u: only ravel-server is stopped, once, and nothing started or restarted by compose start/restart"
else
  fail "u: stop, start and restart calls in ${LOG}: $(grep -E ' (stop|start|restart) ' "${LOG}")"
fi
# The start step: every compose start or up after the fold is exactly the
# no-deps, no-recreate up of ravel-server, and no one-shot service ran again.
if python3 -I - "${LOG}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
fold = next(i for i, l in enumerate(lines) if "catalog fold" in l)
after = lines[fold + 1:]
calls = [l for l in after if l.startswith(("docker-compose-up ", "docker-compose-start "))]
oneshots = [l for l in after if l.startswith("docker-oneshot ")
            or (l.startswith(("docker-compose-up ", "docker-compose-start "))
                and any(s in l.split() for s in ("createbuckets", "ravel-qualify")))]
sys.exit(0 if calls == ["docker-compose-up -d --no-deps --no-recreate ravel-server"] and not oneshots else 1)
PY
  pass "u: the start step ran exactly docker compose up -d --no-deps --no-recreate ravel-server"
else
  fail "u: compose start/up calls after the fold in ${LOG}: $(grep -E '^docker-(compose-up|compose-start|oneshot) ' "${LOG}")"
fi
expect_no_log "u: no one-shot service started after the fold" "docker-oneshot started"
expect_log "u: the fold ran while ravel-server was stopped" "docker-state fold ravel-server=stopped"
expect_log "u: the folded read-back reads the Ravel row of ingest.json" "metricsbench_ingest --readback-only --ingest-report ${OUT}/ingest.json --readback-system ravel --workload "
expect_log "u: the folded read-back uses the ingest step's workload, profile, anchor, URL and token env" "--profile ci --anchor-ms ${ANCHOR} --ravel-query-url http://127.0.0.1:4318 --ravel-bearer-token-env METRICSBENCH_RAVEL_TOKEN --readback-timeout-secs 30 --readback-settle-secs 60"
expect_no_log "u: the ingest step passes no settle flag" "--max-ingest-secs 600 --readback-timeout-secs 30 --readback-settle-secs"
INGEST_BIN_SRC="${HERE}/../../../crates/ravel-bench/src/bin/metricsbench_ingest.rs"
if grep -A1 -F '#[arg(long, default_value_t = 60)]' "${INGEST_BIN_SRC}" | grep -qF 'readback_settle_secs: u64,'; then
  pass "u: 60 s is the bin's --readback-settle-secs default the ingest step runs with"
else
  fail "u: readback_settle_secs does not default to 60 in ${INGEST_BIN_SRC}"
fi
# The order of the calls themselves: the stop, the fold, the start and its
# readiness probe, then the scrape, read-back and scrape, all after the
# memory summary's inspect.
if python3 -I - "${LOG}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
def first(s, after=-1):
    return next(i for i, l in enumerate(lines) if i > after and s in l)
mem = max(i for i, l in enumerate(lines) if l.startswith("docker inspect id-"))
stop = first(" stop ravel-server")
fold = first("catalog fold")
start = first("docker-compose-up -d --no-deps --no-recreate ravel-server")
ready = first("/api/v1/status/buildinfo", start)
before = first("/metrics", start)
rb = first("--readback-only")
after = first("/metrics", rb)
sys.exit(0 if mem < stop < fold < start < ready < before < rb < after else 1)
PY
  pass "u: calls ran memory, stop, fold, start, readiness, scrape, read-back, scrape"
else
  fail "u: calls out of order in ${LOG}"
fi
HOUR_NOW=$(($(date -u +%s) / 3600))
check "u: fold stamped with its report's figures" 'j("stamps.json")["ravel_catalog_fold_writers_stopped"]["entry_count"] == 420 and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["buckets_folded"] == 2 and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["no_op"] is False and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["precondition"] == "met"'
check "u: fold stamped with sealed and watermark hours at the ingest end hour" 'abs(j("stamps.json")["ravel_catalog_fold_writers_stopped"]["seal_through_hour"] - '"${HOUR_NOW}"') <= 1 and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["watermark_hour"] >= j("stamps.json")["ravel_catalog_fold_writers_stopped"]["ingest_end_hour"]'
check "u: stop and start stamped" 'j("stamps.json")["ravel_restart"] == "stopped: docker compose stop ravel-server before the fold; started: docker compose up -d --no-deps --no-recreate ravel-server after it, then ravel answered 200 on /api/v1/status/buildinfo"'
check "u: folded read-back stamped with exit, match, series query_ms, deltas and the catalog GET verdict" 'j("stamps.json")["readback_folded"] == {"exit_code": 0, "matches_expected": True, "error": None, "series_query_ms": 12, "store_get_calls_delta": 100, "commit_record_cache_entries_delta": 3, "fetch_cache_misses_delta": 95, "catalog_get_delta": 5, "data_objects_listed": 300, "data_objects_flushed": 200, "folded_catalog_get_max_fraction": "0.05", "folded_catalog_get_floor": 64, "folded_catalog_get_limit": 79, "catalog_get_verdict": "within"}'
check "u: data_objects_listed is ravel-objects.json's data count, the limit 64 + 0.05 x 300 = 79" 'j("stamps.json")["readback_folded"]["data_objects_listed"] == j("ravel-objects.json")["data"]["count"] == 300 and j("stamps.json")["readback_folded"]["folded_catalog_get_limit"] == 79'
check "u: data_objects_flushed sums the five disjoint metrics reasons only" 'j("stamps.json")["readback_folded"]["data_objects_flushed"] == 40 + 100 + 20 + 30 + 10'
check "u: readback-folded.json is the bin's report" 'j("readback-folded.json")["system"] == "ravel"'

# u1: a fold that sealed nothing.
mapfile -t args < <(full_args "${TMP}/u1/out")
STUB_FOLD_SEAL=None run_case u1 "${args[@]}"
expect_rc u1 70
expect_err "u1: message names step fold" "step fold failed: fold sealed nothing: seal_through_hour=None"
check "u1: stamps name step fold, fold stamped failed" 'j("stamps.json")["failed_step"] == "fold" and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["precondition"].startswith("fold sealed nothing")'
check "u1: stop done, start not, the server stamped stopped and not started again" 'j("stamps.json")["steps_completed"][-2:] == ["memory","stop"] and j("stamps.json")["ravel_restart"] == "stopped: docker compose stop ravel-server before the fold; not started again"'
expect_log "u1: the fold ran while ravel-server was stopped" "docker-state fold ravel-server=stopped"
expect_no_log "u1: no start" "up -d --no-deps --no-recreate ravel-server"
expect_no_log "u1: no compose start" "docker-compose-start "
expect_no_log "u1: no folded read-back" "--readback-only"

# u2: a fold whose watermark is before the hour ingest ended in.
mapfile -t args < <(full_args "${TMP}/u2/out")
STUB_FOLD_WATERMARK_LAG=2 run_case u2 "${args[@]}"
expect_rc u2 70
expect_err "u2: message names step fold and the watermark" "step fold failed: watermark_hour=Some("
check "u2: stamps name step fold, watermark before the ingest end hour" 'j("stamps.json")["failed_step"] == "fold" and j("stamps.json")["ravel_catalog_fold_writers_stopped"]["watermark_hour"] < j("stamps.json")["ravel_catalog_fold_writers_stopped"]["ingest_end_hour"]'
expect_no_log "u2: no start" "up -d --no-deps --no-recreate ravel-server"
expect_no_log "u2: no compose start" "docker-compose-start "

# u2a: docker compose stop fails: no fold, no start.
mapfile -t args < <(full_args "${TMP}/u2a/out")
STUB_STOP_EXIT=1 run_case u2a "${args[@]}"
expect_rc u2a 70
expect_err "u2a: message names step stop" "step stop failed: docker compose stop ravel-server exited 1"
check "u2a: stamps name step stop, the failed stop stamped" 'j("stamps.json")["failed_step"] == "stop" and j("stamps.json")["ravel_restart"] == "not stopped: docker compose stop ravel-server exited 1; no fold ran" and j("stamps.json")["steps_completed"][-1] == "memory"'
expect_no_log "u2a: no fold" "catalog fold"

# u2b: starting ravel-server fails after the fold: no read-back.
mapfile -t args < <(full_args "${TMP}/u2b/out")
STUB_START_EXIT=1 run_case u2b "${args[@]}"
expect_rc u2b 70
expect_err "u2b: message names step start" "step start failed: docker compose up -d --no-deps --no-recreate ravel-server exited 1"
check "u2b: stamps name step start after stop and fold, the failed start stamped" 'j("stamps.json")["failed_step"] == "start" and j("stamps.json")["steps_completed"][-3:] == ["memory","stop","fold"] and j("stamps.json")["ravel_restart"] == "stopped: docker compose stop ravel-server before the fold; docker compose up -d --no-deps --no-recreate ravel-server after it exited 1"'
expect_no_log "u2b: no folded read-back" "--readback-only"

# u3: the folded read-back fails.
mapfile -t args < <(full_args "${TMP}/u3/out")
STUB_READBACK_ONLY_EXIT=1 run_case u3 "${args[@]}"
expect_rc u3 70
expect_err "u3: message names step readback-folded and the error" "step readback-folded failed: metricsbench_ingest --readback-only exited 1: query series failed: 503"
check "u3: stamps name step readback-folded, exit 1 and the error stamped" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["exit_code"] == 1 and j("stamps.json")["readback_folded"]["error"] == "query series failed: 503" and "start" in j("stamps.json")["steps_completed"]'
expect_file u3 ravel-metrics-after-folded-readback.txt

# u4: the scrape after the folded read-back fails (scrape 1 is fold-state's,
# scrape 2 the one after the query step).
mapfile -t args < <(full_args "${TMP}/u4/out")
STUB_METRICS_DOWN_FROM=4 run_case u4 "${args[@]}"
expect_rc u4 70
expect_err "u4: message names step readback-folded and the scrape" "step readback-folded failed: the after scrape"
check "u4: no delta without the second scrape" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["store_get_calls_delta"] is None'

# u5: Ravel's own read-back failed at ingest: the four steps still run.
mapfile -t args < <(full_args "${TMP}/u5/out")
STUB_INGEST_READBACK_FAIL=ravel run_case u5 "${args[@]}"
expect_rc u5 70
check "u5: stamps name ingest, the four steps done after memory" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["steps_completed"][-5:] == ["memory","stop","fold","start","readback-folded"] and j("stamps.json")["failed_after_ingest"] == []'
check "u5: the folded read-back matched" 'j("stamps.json")["readback_folded"]["matches_expected"] is True'
expect_err "u5: message names step ingest" "step ingest failed: metricsbench_ingest exited 1: ravel: read-back failed"

# u6: the same, and the folded read-back fails too: listed after ingest.
mapfile -t args < <(full_args "${TMP}/u6/out")
STUB_INGEST_READBACK_FAIL=ravel STUB_READBACK_ONLY_EXIT=1 run_case u6 "${args[@]}"
expect_rc u6 70
check "u6: stamps name ingest, readback-folded failed after it" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["failed_after_ingest"] == ["readback-folded"]'

# u7: the Ravel row has a row_failure: the four steps are skipped and say why.
mapfile -t args < <(full_args "${TMP}/u7/out")
STUB_INGEST_READBACK_FAIL=victoriametrics STUB_INGEST_ROW_FAILURE=ravel run_case u7 "${args[@]}"
expect_rc u7 70
check "u7: stamps name ingest, no stop, fold, start or readback-folded" 'j("stamps.json")["failed_step"] == "ingest" and not {"stop","fold","start","readback-folded"} & set(j("stamps.json")["steps_completed"]) and "memory" in j("stamps.json")["steps_completed"]'
check "u7: each of the three stamps skipped for the row_failure" 'all(j("stamps.json")[k].startswith("skipped: the ravel row has a row_failure") for k in ("ravel_catalog_fold_writers_stopped","ravel_restart","readback_folded"))'
expect_no_log "u7: no fold" "catalog fold"
expect_no_log "u7: no folded read-back" "--readback-only"

# u8: the Ravel row stalled: skipped the same way.
mapfile -t args < <(full_args "${TMP}/u8/out")
STUB_INGEST_STALL=ravel run_case u8 "${args[@]}"
expect_rc u8 70
check "u8: each of the three stamps skipped for the stall" 'all(j("stamps.json")[k].startswith("skipped: the ravel row stalled") for k in ("ravel_catalog_fold_writers_stopped","ravel_restart","readback_folded"))'
expect_no_log "u8: no fold" "catalog fold"

# u9: the diagnostic mode (case f) runs none of them.
OUT="${TMP}/f/out"
LOG="${TMP}/f/calls.log"
check "u9: diagnostic stamps the three not applicable" 'all(j("stamps.json")[k] == "not applicable: diagnostic run starts no Ravel" for k in ("ravel_catalog_fold_writers_stopped","ravel_restart","readback_folded")) and not {"stop","fold","start","readback-folded"} & set(j("stamps.json")["steps_completed"])'
expect_no_log "u9: diagnostic runs no fold" "catalog fold"
expect_no_log "u9: diagnostic stops nothing" "stop ravel-server"
expect_no_log "u9: diagnostic starts no ravel-server" "up -d --no-deps --no-recreate ravel-server"
expect_no_log "u9: diagnostic runs no compose start" "docker-compose-start "
expect_no_log "u9: diagnostic runs no folded read-back" "--readback-only"

# u10: the real-S3 lane (case s) folds through the same service, with its overlay.
LOG="${TMP}/s/calls.log"
expect_log "u10: the real-S3 fold runs on ravel-qualify with real-s3.yml" "real-s3.yml run --rm --no-deps -T ravel-qualify --store s3 --tenant-hash-unkeyed catalog fold"

# u11: many store GETs, nearly all fetch-cache misses: 2000 GETs less 1995
# misses leaves 5 catalog GETs against 0.05 x 300 listed data objects, a pass.
mapfile -t args < <(full_args "${TMP}/u11/out")
STUB_GETS_PER_SCRAPE=2000 STUB_FETCH_MISSES_PER_SCRAPE=1995 run_case u11 "${args[@]}"
expect_rc u11 0
check "u11: catalog_get_delta is the GET delta less the fetch misses, within the fraction" 'j("stamps.json")["readback_folded"]["store_get_calls_delta"] == 2000 and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 5 and j("stamps.json")["readback_folded"]["data_objects_listed"] == 300 and j("stamps.json")["readback_folded"]["catalog_get_verdict"] == "within" and "readback-folded" in j("stamps.json")["steps_completed"]'

# u12: about one catalog GET per data object (2200 less 2000 = 200 against
# 200 listed objects), as an unfolded read resolves commit records: a breach
# of 64 + 0.05 x 200 = 74. The flush counters sum to 20 here, so a limit
# taken from them (64 + 0.05 x 20 = 65, at or above 20) would pass this read
# as not discriminating.
mapfile -t args < <(full_args "${TMP}/u12/out")
STUB_LIST_DATA=200 STUB_FLUSH_PAIRS="size:4 age:10 age_adaptive:2 age_floor:3 arrival:1" \
  STUB_GETS_PER_SCRAPE=2200 STUB_FETCH_MISSES_PER_SCRAPE=2000 run_case u12 "${args[@]}"
expect_rc u12 70
expect_err "u12: message names step readback-folded and the breach" "step readback-folded failed: folded read-back catalog GETs: breach: catalog_get_delta 200 exceeds 64 + 0.05 x data_objects_listed 200 = 74"
check "u12: stamps name readback-folded and keep the figures and the verdict" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 200 and j("stamps.json")["readback_folded"]["data_objects_listed"] == 200 and j("stamps.json")["readback_folded"]["data_objects_flushed"] == 20 and j("stamps.json")["readback_folded"]["catalog_get_verdict"].startswith("breach:") and "readback-folded" not in j("stamps.json")["steps_completed"]'

# u13: a breach after a failed ingest verification: listed after ingest.
mapfile -t args < <(full_args "${TMP}/u13/out")
STUB_INGEST_READBACK_FAIL=ravel STUB_GETS_PER_SCRAPE=2200 STUB_FETCH_MISSES_PER_SCRAPE=2000 run_case u13 "${args[@]}"
expect_rc u13 70
check "u13: stamps name ingest, readback-folded failed after it with its figures" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["failed_after_ingest"] == ["readback-folded"] and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 200 and j("stamps.json")["readback_folded"]["catalog_get_verdict"].startswith("breach:")'

# u14: no fetch-miss counter in the scrapes: could not measure, a failure.
mapfile -t args < <(full_args "${TMP}/u14/out")
STUB_NO_FETCH_MISSES=1 run_case u14 "${args[@]}"
expect_rc u14 70
expect_err "u14: message names step readback-folded and the absent counter" 'step readback-folded failed: folded read-back catalog GETs: could not measure: no ravel_cache_misses_total{cache="fetch"} delta'
check "u14: catalog_get_delta null, both object counts still stamped" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["catalog_get_delta"] is None and j("stamps.json")["readback_folded"]["data_objects_listed"] == 300 and j("stamps.json")["readback_folded"]["data_objects_flushed"] == 200 and j("stamps.json")["readback_folded"]["catalog_get_verdict"].startswith("could not measure")'

# u15: no metrics flush counters in the at-query scrape: the cross-check is
# stamped null and the check, which does not use it, still passes.
mapfile -t args < <(full_args "${TMP}/u15/out")
STUB_NO_FLUSH_COUNTERS=1 run_case u15 "${args[@]}"
expect_rc u15 0
check "u15: data_objects_flushed null, the verdict from the listed count" 'j("stamps.json")["readback_folded"]["data_objects_flushed"] is None and j("stamps.json")["readback_folded"]["data_objects_listed"] == 300 and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 5 and j("stamps.json")["readback_folded"]["catalog_get_verdict"] == "within"'

# u15a: ravel-objects.json gone by the read-back: could not measure, a failure.
mapfile -t args < <(full_args "${TMP}/u15a/out")
STUB_OBJECTS_DOC_AT="${TMP}/u15a/out/ravel-objects.json" run_case u15a "${args[@]}"
expect_rc u15a 70
expect_err "u15a: message names step readback-folded and the unreadable listing" "step readback-folded failed: folded read-back catalog GETs: could not measure: ravel-objects.json could not be read (FileNotFoundError)"
check "u15a: data_objects_listed null, no limit, the cross-check still stamped" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["data_objects_listed"] is None and j("stamps.json")["readback_folded"]["folded_catalog_get_limit"] is None and j("stamps.json")["readback_folded"]["data_objects_flushed"] == 200'

# u15b: ravel-objects.json without data.count: could not measure, a failure.
mapfile -t args < <(full_args "${TMP}/u15b/out")
STUB_OBJECTS_DOC_AT="${TMP}/u15b/out/ravel-objects.json" STUB_OBJECTS_DOC='{"data": {"bytes": 1}}' \
  run_case u15b "${args[@]}"
expect_rc u15b 70
expect_err "u15b: message names step readback-folded and the missing count" "step readback-folded failed: folded read-back catalog GETs: could not measure: no data.count in ravel-objects.json"
check "u15b: data_objects_listed null" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["data_objects_listed"] is None'

# u16: a looser fraction is passed through and stamped; with u13's figures
# its limit 64 + 1 x 300 = 364 is above the 300 objects: not discriminating.
mapfile -t args < <(full_args "${TMP}/u16/out")
STUB_GETS_PER_SCRAPE=2200 STUB_FETCH_MISSES_PER_SCRAPE=2000 run_case u16 "${args[@]}" --folded-catalog-get-max-fraction 1
expect_rc u16 0
check "u16: the fraction stamped as given, limit 364 not discriminating against 300 objects" 'j("stamps.json")["readback_folded"]["folded_catalog_get_max_fraction"] == "1" and j("stamps.json")["readback_folded"]["folded_catalog_get_limit"] == 364 and j("stamps.json")["readback_folded"]["catalog_get_verdict"].startswith("not discriminating: limit 364 is at or above the 300 data objects listed")'

# u17-u19: a malformed or out-of-range fraction is a usage error.
for bad in u17:abc u18:1.5 u19:-0.1; do
  name="${bad%%:*}"
  mapfile -t args < <(full_args "${TMP}/${name}/out")
  run_case "${name}" "${args[@]}" --folded-catalog-get-max-fraction "${bad#*:}"
  expect_rc "${name}" 64
  expect_err "${name}: message names the flag and the value" "--folded-catalog-get-max-fraction takes a decimal from 0 to 1, got '${bad#*:}'"
  expect_no_log "${name}: nothing started" "compose"
done

# u20: a ci-sized run, 20 listed data objects. 35 catalog GETs exceed
# 0.05 x 20 and even the 20 objects, but the limit 64 + 0.05 x 20 = 65 is
# above 20: not discriminating, a pass. The flush counters say 200 here, which
# would make the limit 74 and the verdict within.
mapfile -t args < <(full_args "${TMP}/u20/out")
STUB_LIST_DATA=20 STUB_GETS_PER_SCRAPE=130 run_case u20 "${args[@]}"
expect_rc u20 0
check "u20: the floor default 64 and the limit 65 stamped against 20 listed data objects" 'j("stamps.json")["readback_folded"]["folded_catalog_get_floor"] == 64 and j("stamps.json")["readback_folded"]["folded_catalog_get_limit"] == 65 and j("stamps.json")["readback_folded"]["data_objects_listed"] == 20 and j("stamps.json")["readback_folded"]["data_objects_flushed"] == 200 and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 35'
check "u20: verdict not discriminating, readback-folded done" 'j("stamps.json")["readback_folded"]["catalog_get_verdict"] == "not discriminating: limit 65 is at or above the 20 data objects listed, so a folded and an unfolded read cannot be told apart at this profile" and "readback-folded" in j("stamps.json")["steps_completed"] and j("stamps.json")["failed_step"] is None'

# u21, u22: a cardinality-sized run, 15,248 listed data objects, limit
# 64 + 0.05 x 15248 = 826.4: 826 catalog GETs are within, 827 a breach. The
# flush counters stay at 200, which would put the limit at 74.
mapfile -t args < <(full_args "${TMP}/u21/out")
STUB_LIST_DATA=15248 STUB_GETS_PER_SCRAPE=2826 STUB_FETCH_MISSES_PER_SCRAPE=2000 run_case u21 "${args[@]}"
expect_rc u21 0
check "u21: 826 catalog GETs within the limit 826.4 for 15248 listed data objects" 'j("stamps.json")["readback_folded"]["data_objects_listed"] == 15248 and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 826 and j("stamps.json")["readback_folded"]["folded_catalog_get_limit"] == 826.4 and j("stamps.json")["readback_folded"]["catalog_get_verdict"] == "within" and "readback-folded" in j("stamps.json")["steps_completed"]'
mapfile -t args < <(full_args "${TMP}/u22/out")
STUB_LIST_DATA=15248 STUB_GETS_PER_SCRAPE=2827 STUB_FETCH_MISSES_PER_SCRAPE=2000 run_case u22 "${args[@]}"
expect_rc u22 70
expect_err "u22: message names the breach with floor, fraction and limit" "step readback-folded failed: folded read-back catalog GETs: breach: catalog_get_delta 827 exceeds 64 + 0.05 x data_objects_listed 15248 = 826.4"
check "u22: stamps name readback-folded with the breach" 'j("stamps.json")["failed_step"] == "readback-folded" and j("stamps.json")["readback_folded"]["catalog_get_delta"] == 827 and j("stamps.json")["readback_folded"]["catalog_get_verdict"].startswith("breach:")'

# u23: the floor is passed through and stamped: floor 0 on case u's figures
# gives the limit 0.05 x 300 = 15, and 5 catalog GETs are within it.
mapfile -t args < <(full_args "${TMP}/u23/out")
run_case u23 "${args[@]}" --folded-catalog-get-floor 0
expect_rc u23 0
check "u23: floor 0 and limit 15 stamped, within" 'j("stamps.json")["readback_folded"]["folded_catalog_get_floor"] == 0 and j("stamps.json")["readback_folded"]["folded_catalog_get_limit"] == 15 and j("stamps.json")["readback_folded"]["catalog_get_verdict"] == "within"'

# u24-u27: a floor that is not a whole number is a usage error.
for bad in u24:abc u25:-1 u26:1.5 u27:; do
  name="${bad%%:*}"
  mapfile -t args < <(full_args "${TMP}/${name}/out")
  run_case "${name}" "${args[@]}" --folded-catalog-get-floor "${bad#*:}"
  expect_rc "${name}" 64
  expect_err "${name}: message names the flag and the value" "--folded-catalog-get-floor takes a whole number, got '${bad#*:}'"
  expect_no_log "${name}: nothing started" "compose"
done
# --- v: Ravel's objects -------------------------------------------------------------
# Three data objects, three commit records, two qualify probes and four other
# keys, two of those under m/: 12 keys, 8 under t/*/m/. A band of 3:3 holds
# only the data count; 8:12 holds the m/ count and the total but not it.
V_LISTING="${TMP}/v-listing.json"
cat >"${V_LISTING}" <<'JSON'
{"Contents": [
  {"Key": "t/h1/m/l0/0001.rseg", "Size": 1000},
  {"Key": "t/h1/m/l0/0002.rseg", "Size": 2000},
  {"Key": "t/h2/m/l0/0003.rseg", "Size": 3000},
  {"Key": "t/h1/m/c/0001", "Size": 10},
  {"Key": "t/h1/m/c/0002", "Size": 20},
  {"Key": "t/h2/m/c/0003", "Size": 30},
  {"Key": "sys/qualify/run-1/probe-a", "Size": 1},
  {"Key": "sys/qualify/run-1/probe-b", "Size": 2},
  {"Key": "t/h1/catalog/manifest-1", "Size": 300},
  {"Key": "t/h1/m/admission", "Size": 40},
  {"Key": "t/h1/m/prov", "Size": 50},
  {"Key": "sys/tenancy", "Size": 60}
]}
JSON
V_SPLIT='j("ravel-objects.json")["data"] == {"count": 3, "bytes": 6000} and j("ravel-objects.json")["commit"] == {"count": 3, "bytes": 60} and j("ravel-objects.json")["qualify"] == {"count": 2, "bytes": 3} and j("ravel-objects.json")["other"] == {"count": 4, "bytes": 450} and j("ravel-objects.json")["total"] == {"count": 12, "bytes": 6513}'

mapfile -t args < <(full_args "${TMP}/v/out")
STUB_LISTING="${V_LISTING}" run_case v "${args[@]}"
expect_rc v 0
expect_file v ravel-objects.json
expect_file v ravel-objects-listing.json
check "v: every kind split, total is the sum" "${V_SPLIT}"
check "v: the same figures stamped" 'j("stamps.json")["ravel_objects"] == j("ravel-objects.json") and j("stamps.json")["ravel_objects"]["bucket"] == "ravel-metricsbench"'
check "v: no band, stamped as not set" 'j("ravel-objects.json")["band"].startswith("not set")'
expect_err "v: message says no band was set" "no --ravel-objects-band set, counts stamped only"
expect_log "v: the portable stack lists ravel-metricsbench through s3s" "run --rm --no-deps -T --entrypoint aws createbuckets --endpoint-url http://s3s:9000 s3api list-objects-v2 --output json --bucket ravel-metricsbench"
check "v: objects runs between mimir-readiness and capture" 'j("stamps.json")["steps_completed"][7:10] == ["mimir-readiness","objects","capture"]'

mapfile -t args < <(full_args "${TMP}/v1/out")
STUB_LISTING="${V_LISTING}" run_case v1 "${args[@]}" --ravel-objects-band 3:3
expect_rc "v1 band holds the data count" 0
check "v1: band verdict ok on the data count" 'j("ravel-objects.json")["band"] == {"min": 3, "max": 3, "counts": "data", "verdict": "ok"}'

mapfile -t args < <(full_args "${TMP}/v2/out")
STUB_LISTING="${V_LISTING}" run_case v2 "${args[@]}" --ravel-objects-band 8:12
expect_rc "v2 band excludes the data count" 70
expect_err "v2: message names step objects and the count" "step objects failed: data 3 (6000 B)"
expect_err "v2: message says below the band" "data count 3 below --ravel-objects-band 8:12"
check "v2: stamps name step objects, figures still stamped" 'j("stamps.json")["failed_step"] == "objects" and j("stamps.json")["ravel_objects"]["band"]["verdict"] == "below" and j("stamps.json")["ravel_objects"]["data"]["count"] == 3'
expect_no_log "v2: capture not run" "capture --out"
expect_log "v2: torn down" " down -v"

for bad in 3 a:b 3:2 -1:4 3:4:5; do
  mapfile -t args < <(full_args "${TMP}/v3-${bad//:/_}/out")
  run_case "v3-${bad//:/_}" "${args[@]}" --ravel-objects-band "${bad}"
  expect_rc "v3 band '${bad}'" 64
  expect_no_log "v3 band '${bad}': nothing started" "compose -f"
done

run_case v4 --profile history --bin-dir "${BIN}" --out-dir "${TMP}/v4/out" --anchor-ms "${ANCHOR}" \
  --diagnostic-prometheus-max-samples 1000 --ravel-objects-band 1:2
expect_rc "v4 band in the diagnostic mode" 64

# A failed ingest verification: the step still runs; a band it fails is
# listed after ingest and the run ends there naming ingest.
mapfile -t args < <(full_args "${TMP}/v5/out")
STUB_LISTING="${V_LISTING}" STUB_INGEST_READBACK_FAIL=mimir run_case v5 "${args[@]}" --ravel-objects-band 8:12
expect_rc "v5 failed ingest, band excludes" 70
check "v5: ingest named, objects failed after it" 'j("stamps.json")["failed_step"] == "ingest" and j("stamps.json")["failed_after_ingest"] == ["objects"]'
expect_no_log "v5: capture not run" "capture --out"

mapfile -t args < <(full_args "${TMP}/v6/out")
STUB_LIST_EXIT=254 run_case v6 "${args[@]}"
expect_rc "v6 listing fails" 70
check "v6: stamps name step objects" 'j("stamps.json")["failed_step"] == "objects" and j("stamps.json")["ravel_objects"] is None'

printf '%s\n' '{"Contents": [{"Key": "t/h1/m/l0/a.rseg", "Size": 1}], "NextToken": "abc"}' >"${TMP}/v7-listing.json"
mapfile -t args < <(full_args "${TMP}/v7/out")
STUB_LISTING="${TMP}/v7-listing.json" run_case v7 "${args[@]}"
expect_rc "v7 truncated listing" 70
expect_err "v7: message says truncated" "the listing is truncated"

mapfile -t args < <(full_args "${TMP}/v8/out")
STUB_LISTING="${V_LISTING}" run_case v8 "${args[@]}" --real-s3-bucket-prefix mb-run1 --real-s3-region us-east-1
expect_rc "v8 real-S3 lane" 0
expect_log "v8: the real-S3 lane lists PREFIX-ravel" "run --rm --no-deps -T --entrypoint aws createbuckets s3api list-objects-v2 --output json --bucket mb-run1-ravel"
expect_no_log "v8: no endpoint override" "--endpoint-url"
check "v8: stamped bucket mb-run1-ravel" 'j("stamps.json")["ravel_objects"]["bucket"] == "mb-run1-ravel"'

# --- w: the scrapes around the query step --------------------------------------
OUT="${TMP}/a/out"
LOG="${TMP}/a/calls.log"
for f in ravel-metrics-at-query.txt mimir-metrics-at-query.txt ravel-metrics-after-query.txt mimir-metrics-after-query.txt; do
  expect_file w "$f"
done
check "w: query_scrapes stamps ok for all four" 'j("stamps.json")["query_scrapes"] == {"ravel_before": "ok", "mimir_before": "ok", "ravel_after": "ok", "mimir_after": "ok"}'
if python3 -I - "${LOG}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
def idx(s):
    return [i for i, l in enumerate(lines) if l.startswith("curl ") and s in l]
ravel, mimir = idx("127.0.0.1:4318/metrics"), idx("127.0.0.1:9009/metrics")
query = next(i for i, l in enumerate(lines) if l.startswith("metricsbench_query --"))
validate = next(i for i, l in enumerate(lines) if l.startswith("metricsbench_report validate"))
stop = next(i for i, l in enumerate(lines) if " stop ravel-server" in l)
ok = (len(mimir) == 2 and len(ravel) == 4
      and ravel[0] < query and mimir[0] < query
      and query < ravel[1] < validate and query < mimir[1] < validate and validate < stop)
sys.exit(0 if ok else 1)
PY
  pass "w: before pair ahead of the query run, after pair after it and ahead of validate and stop"
else
  fail "w: scrapes around the query step out of order in ${LOG}"
fi

for mode in "refused:1:failed: curl exit 7" "503:2:failed: HTTP 503" "empty:3:failed: empty body"; do
  how="${mode%%:*}"
  rest="${mode#*:}"
  n="w${rest%%:*}"
  reason="${rest#*:}"
  mapfile -t args < <(full_args "${TMP}/${n}/out")
  STUB_MIMIR_METRICS="${how}" run_case "${n}" "${args[@]}"
  expect_rc "${n} Mimir scrape ${how}" 0
  check "${n}: both Mimir scrapes stamped ${reason}, Ravel's ok, the run passed" 'j("stamps.json")["query_scrapes"] == {"ravel_before": "ok", "mimir_before": "'"${reason}"'", "ravel_after": "ok", "mimir_after": "'"${reason}"'"} and j("stamps.json")["status"] == "passed"'
  expect_no_file "${n}" mimir-metrics-at-query.txt
  expect_no_file "${n}" mimir-metrics-after-query.txt
  expect_file "${n}" ravel-metrics-after-query.txt
done

mapfile -t args < <(full_args "${TMP}/w4/out")
STUB_MIMIR_METRICS=refused STUB_MEM_ravel_server=2GiB run_case w4 "${args[@]}" --rss-band ravel-server:0:1073741824
expect_rc "w4 Mimir refused plus an --rss-band breach" 75
check "w4: stamps name memory, the Mimir scrapes stamped" 'j("stamps.json")["failed_step"] == "memory" and j("stamps.json")["query_scrapes"]["mimir_after"] == "failed: curl exit 7"'

OUT="${TMP}/q4/out"
check "q4: a refused query takes no after pair, stamped not taken" 'j("stamps.json")["query_scrapes"] == {"ravel_before": "ok", "mimir_before": "ok", "ravel_after": "not taken: the query step did not complete", "mimir_after": "not taken: the query step did not complete"}'
expect_no_file q4 ravel-metrics-after-query.txt
expect_no_file q4 mimir-metrics-after-query.txt

OUT="${TMP}/f/out"
LOG="${TMP}/f/calls.log"
check "f: query_scrapes stamped not applicable" 'j("stamps.json")["query_scrapes"] == "not applicable: diagnostic run starts no Ravel or Mimir and runs no query step"'
expect_no_log "f: no scrape of Ravel's /metrics" "127.0.0.1:4318/metrics"
expect_no_log "f: no scrape of Mimir's /metrics" "127.0.0.1:9009/metrics"
expect_no_file f mimir-metrics-at-query.txt

echo
echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
