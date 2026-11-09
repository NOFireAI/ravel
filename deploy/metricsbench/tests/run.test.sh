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
#   x  object event-time spread     -> x (case a's output): ravel-object-spans.json
#                                       written after the fold, its five buckets
#                                       summing exactly to ravel-objects.json's
#                                       data count, distinct ingest hours and
#                                       snapshot parts, the stamp naming the
#                                       inspector commands; x0: a second count;
#                                       x1: no snapshot HEAD, x2: a failed
#                                       catalog list, x3: no fold, x4: a failed
#                                       catalog inspect and x6: a non-JSON
#                                       listing line are stamped failures, no
#                                       file; x5: L1 lines are read; x7: no
#                                       ravel-objects.json is null, not false;
#                                       b, u1, u2b, u3: an earlier end is
#                                       stamped not taken; f: not applicable
#   y  Ravel-only cold pass         -> y: --profile history on a corpus with
#                                       ids the real one lacks; the selection is
#                                       exactly its long_range class plus
#                                       mb_full_range_avg_over_time for the
#                                       profile, five repetitions each, a
#                                       restart before every cold query, the
#                                       order validate, memory, cold pass,
#                                       stop, the regime
#                                       cold-ravel-diagnostic marked non-
#                                       comparable, the five phases per record,
#                                       report.json's timed rows unchanged and
#                                       only the standard run writing it; y1:
#                                       failing cold runs are recorded, exit 0;
#                                       y2: a failed ingest runs none; y3: the
#                                       real corpus's four history entries; y4:
#                                       a failed restart and y5: a failed
#                                       readiness wait are recorded runs; y6: a
#                                       missing record is a named failed run;
#                                       y7: an unloadable corpus is a stamped
#                                       failure; b: not taken
#   z  MB_STAGE1 (issue #3968)      -> z0: ravel.yml's entrypoint is the
#                                       image's ENTRYPOINT plus an empty
#                                       RAVEL_SERVER_EXTRA_FLAGS (rendered by
#                                       compose where the host has it), and a:
#                                       compose up gets it empty, no Stage 1
#                                       step, no comparable=false; z:
#                                       cardinality runs stage1-fold,
#                                       -unrouted, -leaves, -restart, -routed
#                                       and -holders after readback-folded in
#                                       that order, the folds pass exactly
#                                       their listed flags, three recreates
#                                       with --disable-fold (two unrouted, one
#                                       routed) each followed by readiness, a
#                                       catalog inspect stamped before each
#                                       pass, every record its pass label and
#                                       the inspected fold state, the leaves
#                                       requests by name, the compare counts
#                                       per pass, the holders listing,
#                                       download and window check, and
#                                       comparable=false on every provenance;
#                                       a and j: the query step's labels; z1:
#                                       ci (cold not applicable); z2: a
#                                       differing hash is recorded, exit 0;
#                                       z2b: the same two hashes in both
#                                       passes are multi-hash, not differing;
#                                       z3: history refused 65; z4: a bad
#                                       value and z5: the diagnostic mode are
#                                       usage errors; z6: 0 is off; z7: a
#                                       failed rebuild, z8a, z8b, z8: no
#                                       readiness after each recreate, z10: a
#                                       failed download, z12: a leaf part
#                                       before the unrouted pass, z13: fewer
#                                       leaf parts than rebuilt before the
#                                       routed pass and z14: an inspect that
#                                       does not add up exit 70 naming the
#                                       step; z9: a failed ingest skips them;
#                                       z11: real S3 reads PREFIX-ravel; z15:
#                                       a failed routed record, z16: an
#                                       unrouted record pruned by the label
#                                       index, z17: a window mismatch and
#                                       z18: an unchecked window are counted,
#                                       exit 0; z19: a failed scrape before
#                                       the query step labels its records
#                                       unknown; z20: an in-window object
#                                       outside the name's postings and an
#                                       absent metric match the window check;
#                                       z21: incorrect in both passes and
#                                       z22: a routed status change (row 9);
#                                       z23: a statement not route-bound is
#                                       listed; z24: a parts_rebuilt that is
#                                       not a whole number refuses the routed
#                                       pass; z25: a routed prune count that
#                                       is not the expected one is a
#                                       mismatch; z26: a route-bound routed
#                                       record that read no leaf is counted,
#                                       z26u: one not route-bound is not;
#                                       z27: the figures of nightly run
#                                       42495422937 check clean; z28a-f: an
#                                       inspect stamp not written, a leaf part
#                                       count not read back or not a whole
#                                       number refuses each pass; z29a-d: a
#                                       watermark or parts figure not read
#                                       back or not a whole number refuses
#                                       the pass; z30: per-value route
#                                       figures over their bound counted.
#                                       Arms (issue #3969): za: every arm
#                                       on, their steps in order, seven
#                                       recreates (the control's alone on
#                                       the control image), the arm folds'
#                                       flags, the leaf-on extra-hour fold
#                                       from the routed HEAD and the
#                                       leaf-off one from the leafless HEAD,
#                                       the seal wait before compact-tenant,
#                                       the erasure's submit, pending pass,
#                                       maintain container (its key outside
#                                       the out dir) and status in order;
#                                       za1 (z1's run): no arm step, each arm
#                                       stamped skipped; zb (control), zc
#                                       (extra hour), zd (l1), ze (erasure):
#                                       each arm alone, with figures out of
#                                       its bands; zb2: a 6% excess inside
#                                       the spread is not slower; zc2: the
#                                       leaf bytes over every object a tail
#                                       part covers; zf1: a control image
#                                       with no digest 65, zf2: no source
#                                       commit, zf3: arms without MB_STAGE1
#                                       and zf4: an unknown arm 64; zg: a
#                                       routed HEAD that does not read back
#                                       refuses the leaf-on fold (70); zg2:
#                                       a leaf-off fold switching off over
#                                       leaves is refused (70); zh: an
#                                       erasure never COMPLETED ends the run
#                                       at stage1-erasure-rewrite (70), its
#                                       key removed; zi: the erasure sleeps
#                                       to its seal deadline before the
#                                       first poll; zj: an erasure that
#                                       rewrote no part.
#                                       stage1-check.sh: the ci band file
#                                       clean on z1 (arm figures n/a), za,
#                                       zb2, zc2, z20 and z21, out of band
#                                       on z2, z2b, z15, z16, z17, z22, zb,
#                                       zc, zd, ze, zj and an edited copy of
#                                       za, a
#                                       missing cold count, missing,
#                                       duplicated, not finite (a NaN figure
#                                       out of band, a NaN bound 65), a
#                                       profile mismatch (65) and usage (64).
#                                       box-run.sh, sourced: the band file
#                                       per profile (refused for cardinality
#                                       without MB_STAGE1_BANDS) and
#                                       stage1_check's status and kept output
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
REAL_DATE="$(command -v date)"

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
      # A recreate of the stopped container is logged as any other recreate.
      case " $* " in
        *" --force-recreate "*) echo running >"${STUB_STATE}.ravel" ;;
        *)
          [ "${STUB_START_EXIT:-0}" -eq 0 ] || exit "${STUB_START_EXIT}"
          echo running >"${STUB_STATE}.ravel"
          rm -f "${STUB_STATE}.notready"
          exit 0
          ;;
      esac
    fi
    case "$sub" in
      # The Stage 1 recreates of ravel-server: each logs the flags ravel.yml
      # appends to its entrypoint, exit STUB_RECREATE_EXIT; after recreate
      # number STUB_RECREATE_NOT_READY (1-based) ravel's buildinfo answers 503.
      up)
        case " $* " in
          *" --force-recreate "*)
            printf 'docker-env recreate RAVEL_SERVER_EXTRA_FLAGS=%s\n' "${RAVEL_SERVER_EXTRA_FLAGS-<unset>}" >>"${STUB_LOG}"
            printf 'docker-image recreate RAVEL_IMAGE=%s\n' "${RAVEL_IMAGE-<unset>}" >>"${STUB_LOG}"
            [ "${STUB_RECREATE_EXIT:-0}" -eq 0 ] || exit "${STUB_RECREATE_EXIT}"
            n=$(($(cat "${STUB_STATE}.recreates" 2>/dev/null || echo 0) + 1))
            echo "$n" >"${STUB_STATE}.recreates"
            rm -f "${STUB_STATE}.notready"
            [ "${STUB_RECREATE_NOT_READY:-0}" != "$n" ] || : >"${STUB_STATE}.notready"
            exit 0
            ;;
        esac
        [ "${STUB_UP_EXIT:-0}" -eq 0 ] || exit "${STUB_UP_EXIT}"
        printf 'docker-env up S3S_FS_BIN=%s\n' "${S3S_FS_BIN-<unset>}" >>"${STUB_LOG}"
        printf 'docker-env up RAVEL_SERVER_EXTRA_FLAGS=%s\n' "${RAVEL_SERVER_EXTRA_FLAGS-<unset>}" >>"${STUB_LOG}"
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
      # restart (the cold pass): exit STUB_RESTART_EXIT; with
      # STUB_RESTART_NOT_READY, ravel's buildinfo answers 503 until the
      # folded steps start the stopped container again.
      restart)
        [ "${STUB_RESTART_EXIT:-0}" -eq 0 ] || exit "${STUB_RESTART_EXIT}"
        [ -z "${STUB_RESTART_NOT_READY:-}" ] || : >"${STUB_STATE}.notready"
        ;;
      run)
        printf 'docker-image run RAVEL_IMAGE=%s\n' "${RAVEL_IMAGE-<unset>}" >>"${STUB_LOG}"
        case " $* " in
          # The Stage 1 arms' HEAD save and restore (aws s3 cp on
          # createbuckets): a download to /stage1-s3/NAME/ writes the HEAD
          # $STUB_STATE.head holds (default head-0) under the host directory
          # mounted at /stage1-s3; an upload from there puts it back, or
          # "broken" with STUB_HEAD_RESTORE_BROKEN.
          *" s3 cp "*)
            argv_=("$@")
            host="" src="" dst=""
            for ((i = 0; i < ${#argv_[@]}; i++)); do
              case "${argv_[$i]}" in
                -v) [ "${argv_[$((i + 1))]#*:}" != /stage1-s3 ] || host="${argv_[$((i + 1))]%%:*}" ;;
                cp) src="${argv_[$((i + 2))]}" dst="${argv_[$((i + 3))]}" ;;
              esac
            done
            case "$dst" in
              /stage1-s3/*)
                head="${host}${dst#/stage1-s3}h1/catalog/m"
                mkdir -p "$head"
                if [ -s "${STUB_STATE}.head" ]; then cp "${STUB_STATE}.head" "${head}/HEAD"; else echo head-0 >"${head}/HEAD"; fi
                ;;
              *)
                if [ -n "${STUB_HEAD_RESTORE_BROKEN:-}" ]; then
                  echo broken >"${STUB_STATE}.head"
                else
                  cp "${host}${src#/stage1-s3}h1/catalog/m/HEAD" "${STUB_STATE}.head"
                fi
                ;;
            esac
            ;;
          # maintain compact-tenant (services/ravel-cli/src/maintain.rs):
          # the header, 2 hours compacted on shard 0 and 2 parts written;
          # exit STUB_COMPACT_EXIT.
          *" compact-tenant "*)
            [ "${STUB_COMPACT_EXIT:-0}" -eq 0 ] || exit "${STUB_COMPACT_EXIT}"
            echo 'store: s3'                                      # maintain.rs:1134 (store.rs:90-96)
            echo 'tenant: metricsbench'                           # maintain.rs:1153
            echo 'signal: Metrics'                                # maintain.rs:1154
            echo 'shards: 4'                                      # maintain.rs:1155
            echo 'hour_range: [0, 490002]'                        # maintain.rs:1156
            echo 'max_flush_lifetime_ns: 0'                       # maintain.rs:1157-1161
            # maintain.rs:438 (crates/ravel-maintain/src/config.rs:1441-1465)
            echo 'rlog_l1_part_memory_target_bytes: 268435456 (resolved from a memory budget of 1073741824 over 1 concurrent merge; bound by the 268435456-byte floor)'
            echo 'rlog_max_l1_part_bytes: 268435456'              # maintain.rs:439-443
            echo 'rspan_l1_part_memory_target_bytes: 268435456'   # maintain.rs:444-448
            echo 'max_l1_part_bytes: 268435456'                   # maintain.rs:449
            echo 'input_read_concurrency: 32'                     # maintain.rs:1162-1166
            echo 'dry_run: false'                                 # maintain.rs:1168
            echo 'claims: on process_id=stub min_input_bytes=67108864 lease_ms=300000' # maintain.rs:171-176, 1169
            echo 'shard=0 hour=490000 outcome=Compacted parts=1 publish=Published' # maintain.rs:1463-1466
            echo 'shard=0 hour=490001 outcome=Compacted parts=1 publish=Published' # maintain.rs:1463-1466
            printf 'compacted: 2\nalready: 0\nrewrite_present: 0\nnot_sealed: 0\nbelow_min: 0\ntombstoned: 0\nclaim_skipped: 0\nclaim_cancelled: 0\nheld_out_of_window: 0\ngeometry_unsupported: 0\n' # maintain.rs:1232-1241
            printf 'parts (wrote): 2\nwall_time_ms: 40\n'        # maintain.rs:1242-1243
            printf 'failed: 0\nbucket_concurrency: 4\n'           # maintain.rs:1258-1259
            : >"${STUB_STATE}.compacted"
            ;;
          # erase submit (services/ravel-cli/src/erase.rs): with
          # STUB_CLOCK_AT_SUBMIT, the stubbed date reads that instant from
          # here on.
          *" erase submit "*)
            [ "${STUB_ERASE_SUBMIT_EXIT:-0}" -eq 0 ] || exit "${STUB_ERASE_SUBMIT_EXIT}"
            argv_=("$@")
            for ((i = 0; i < ${#argv_[@]}; i++)); do
              [ "${argv_[$i]}" != --request-id ] || id="${argv_[$((i + 1))]}"
            done
            echo "submitted erasure request ${id}"               # erase.rs:138
            echo "  key: t/h1/m/del/${id}.dreq"                  # erase.rs:139 (crates/ravel-commit/src/keys.rs:824-847)
            [ -z "${STUB_CLOCK_AT_SUBMIT:-}" ] || echo "${STUB_CLOCK_AT_SUBMIT}" >"${STUB_STATE}.clock"
            ;;
          # erase status (services/ravel-cli/src/erase.rs print_state):
          # COMPLETED once the maintain container has started, PENDING before
          # it or with STUB_ERASE_PENDING. A completion record's bucket drops
          # are empty (services/ravel-server/src/maintain.rs:3799-3819), so
          # COMPLETED lists no bucket line.
          *" erase status "*)
            argv_=("$@")
            for ((i = 0; i < ${#argv_[@]}; i++)); do
              [ "${argv_[$i]}" != --request-id ] || id="${argv_[$((i + 1))]}"
            done
            if [ -z "${STUB_ERASE_PENDING:-}" ] && [ -e "${STUB_STATE}.maintain" ]; then
              echo "${id}: COMPLETED"                             # erase.rs:764
              echo '  requested_unix_ns: 1764001800000000000'     # erase.rs:765
              echo '  completed_unix_ns: 1764007560000000000'     # erase.rs:766
              echo '  deferral cause: none (completed cleanly)'   # erase.rs:767-770, 729
              echo '  per-bucket dropped counts (0 bucket(s), 0 record(s) total):' # erase.rs:776-779
              : >"${STUB_STATE}.erased"
            else
              echo "${id}: PENDING"                               # erase.rs:742
              echo '  created_unix_ns: 1764001800000000000'       # erase.rs:743
              echo '  predicate: instance=metricsbench-instance-0' # erase.rs:744-750
              echo '  event-time window: none (whole retained history)' # erase.rs:757
              echo '  reason: metricsbench stage1 erasure arm'    # erase.rs:759-761
            fi
            ;;
          *" --mode maintain "*)
            [ "${STUB_MAINTAIN_EXIT:-0}" -eq 0 ] || exit "${STUB_MAINTAIN_EXIT}"
            : >"${STUB_STATE}.maintain"
            echo "id-maintain"
            ;;
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
          # stage1-holders' download: the STUB_LIST_DATA (default 300) data
          # objects catalog list names, written under the host directory
          # mounted at /stage1-objects; exit STUB_SYNC_EXIT.
          *" s3 sync "*)
            [ "${STUB_SYNC_EXIT:-0}" -eq 0 ] || exit "${STUB_SYNC_EXIT}"
            host=""
            while [ $# -gt 0 ]; do
              [ "$1" = -v ] && [ "${2#*:}" = /stage1-objects ] && host="${2%%:*}"
              shift
            done
            mkdir -p "${host}/t/h1/m/l0"
            for ((i = 0; i < ${STUB_LIST_DATA:-300}; i++)); do
              printf 'rseg' >"$(printf '%s/t/h1/m/l0/%06d.rseg' "$host" "$i")"
            done
            ;;
          # ravel-cli catalog list --json and catalog inspect (the spans
          # capture): STUB_LIST_DATA segments (default 300), one JSON
          # document per line, cycling through spans of 0.5, 1.5, 3, 12 and
          # 30 hours over two ingest hours, 100 bytes each, the store header
          # line on stderr as the CLI prints it under --json (on stdout with
          # STUB_SPANS_HEADER_STDOUT); STUB_SPANS_L1 makes every third
          # segment an L1 part; exit STUB_SPANS_LIST_EXIT /
          # STUB_SPANS_INSPECT_EXIT. Inspect prints a HEAD at watermark hour
          # 490001 with three parts; the first STUB_INSPECT_LEAF_PARTS
          # (default 0) of them carry a label_value leaf, and once a
          # --rebuild-key-index fold has run the first
          # STUB_INSPECT_REBUILT_LEAF_PARTS (default 3). After the extra hour
          # folds, a fourth part over 490002..=490002 (490000..=490002 with
          # STUB_TAIL_SPAN) carries a leaf with STUB_EXTRA_UNCOVERED
          # uncovered entries.
          # STUB_INSPECT_HEAD_ABSENT prints the line the CLI prints for a
          # tenant with no snapshot HEAD; STUB_INSPECT_PARTS_LINE replaces the
          # parts count the header states.
          *" catalog list "*)
            printf 'docker-state catalog-list ravel-server=%s\n' "$(cat "${STUB_STATE}.ravel" 2>/dev/null || echo running)" >>"${STUB_LOG}"
            [ "${STUB_SPANS_LIST_EXIT:-0}" -eq 0 ] || exit "${STUB_SPANS_LIST_EXIT}"
            [ -z "${STUB_SPANS_OBJECTS_GONE:-}" ] || rm -f "${STUB_SPANS_OBJECTS_GONE}"
            if [ -n "${STUB_SPANS_HEADER_STDOUT:-}" ]; then echo 'store: s3'; else echo 'store: s3' >&2; fi
            l1="${STUB_SPANS_L1:-0}" extra=0
            [ ! -e "${STUB_STATE}.compacted" ] || l1=1
            [ ! -e "${STUB_STATE}.extra" ] || extra=1
            python3 -I -c 'import json,sys; n=int(sys.argv[1]); l1=sys.argv[2]=="1"; x=sys.argv[3]=="1"; H=3600*10**9; spans=[H//2,3*H//2,3*H,12*H,30*H]; print("\n".join(json.dumps({"key":f"t/h1/m/l0/{i:06d}.rseg","level":"L1" if l1 and i%3==0 else "L0","shard":i%4,"ingest_hour_bucket":490002 if x and i%10==9 else 490000+i%2,"object_size":100,"min_event_ts_ns":10**18,"max_event_ts_ns":10**18+spans[i%5]}) for i in range(n)))' "${STUB_LIST_DATA:-300}" "$l1" "$extra"
            ;;
          *" catalog inspect "*)
            [ "${STUB_SPANS_INSPECT_EXIT:-0}" -eq 0 ] || exit "${STUB_SPANS_INSPECT_EXIT}"
            # services/ravel-cli/src/catalog.rs render_inspect, line by line.
            echo 'store: s3'                                      # catalog.rs:849 (store.rs:90-96)
            if [ -n "${STUB_INSPECT_HEAD_ABSENT:-}" ]; then
              echo 'HEAD at t/h1/m/head is absent or unreadable: not found' # catalog.rs:863
            else
              wm=490001 parts="0 1 2" tail_uncovered="${STUB_EXTRA_UNCOVERED:-0}" tail_lo=490002
              [ ! -e "${STUB_STATE}.extra" ] || { wm=490002 parts="0 1 2 3"; }
              [ -z "${STUB_TAIL_SPAN:-}" ] || tail_lo=490000
              # Once the erasure completed, part 0 (over the subject's hours)
              # is rewritten: a new key and blake3, and a new label_value
              # leaf unless STUB_ERASURE_STALE keeps the old one;
              # STUB_ERASURE_UNCHANGED rewrites nothing.
              rewritten=""
              [ ! -e "${STUB_STATE}.erased" ] || [ -n "${STUB_ERASURE_UNCHANGED:-}" ] || rewritten=0
              printf 'format_version: 1\n'                        # catalog.rs:870
              printf 'tenant_hash: %064x\n' 7                     # catalog.rs:871-874
              printf 'signal: metrics (1)\n'                      # catalog.rs:878-882, 971-992
              printf 'shard_count: 4\n'                           # catalog.rs:883
              printf 'watermark_hour: %s\n' "$wm"                 # catalog.rs:884
              printf 'folder_id: 0b5e2c1a-4d6f-4e3a-9b7c-1f2e3d4c5b6a\n' # catalog.rs:885-888, 1005-1009
              printf 'created_unix_ns: 1764000000000000000\n'     # catalog.rs:889
              printf 'parts: %s\n' "${STUB_INSPECT_PARTS_LINE:-$(echo $parts | wc -w)}" # catalog.rs:890
              leaf_parts="${STUB_INSPECT_LEAF_PARTS:-0}"
              [ ! -e "${STUB_STATE}.rebuilt" ] || leaf_parts="${STUB_INSPECT_REBUILT_LEAF_PARTS:-3}"
              for i in $parts; do
                refs=0 lo=490000 hi=490001 unc=0 key="t/h1/m/catalog/part-${i}" b3=$((i + 1)) leaf="t/h1/m/ki/${i}-v"
                [ "$i" -ge "$leaf_parts" ] || refs=1
                [ "$i" -ne 3 ] || { refs=1 lo="$tail_lo" hi=490002 unc="$tail_uncovered"; }
                if [ "$i" = "$rewritten" ]; then
                  key="${key}-e" b3=$((b3 + 224))
                  [ -n "${STUB_ERASURE_STALE:-}" ] || leaf="${leaf}-e"
                fi
                # catalog.rs:897-904
                printf '  part[%s] range=%s..=%s key=%s blake3=%064x size=1000 entry_count=10\n' "$i" "$lo" "$hi" "$key" "$b3"
                printf '    column_stats (field 7): ABSENT\n'     # catalog.rs:913-916
                if [ "$refs" -eq 0 ]; then
                  printf '    key_index (field 8): ABSENT\n'      # catalog.rs:957
                else
                  printf '    key_index (field 8): 1\n'           # catalog.rs:960
                  # catalog.rs:961-965; a metrics part's only key-index field
                  # is label_value (crates/ravel-catalog/src/fold.rs:1341).
                  printf '      field=label_value slice=0x0000000000000000..=0xffffffffffffffff entry_count=10 uncovered_count=%s key=%s\n' "$unc" "$leaf"
                fi
                # catalog.rs:924-932
                printf '    header: format_version=1 min_hour=%s watermark_hour=%s shard_count=4 entry_count=10 entries_uncompressed_len=640\n' "$lo" "$hi"
                printf '    entries (decoded): 10\n'              # catalog.rs:933-936
              done
            fi
            ;;
          # ravel-cli catalog fold on the ravel-qualify service, its report
          # written by foldreport below: the text report, the hours in the
          # clock's current hour bucket. It logs ravel-server's state as it
          # runs. With --json (the Stage 1 folds) the JSON report, nothing on
          # stderr: with --rebuild-key-index the rebuild's, STUB_STAGE1_LEAVES
          # (default 12) leaves and parts_rebuilt STUB_STAGE1_PARTS_REBUILT
          # (default 3, as JSON text), exit STUB_STAGE1_LEAVES_EXIT; otherwise
          # the fold's, exit STUB_STAGE1_FOLD_EXIT. Each publishes a HEAD into
          # $STUB_STATE.head, named "-leaves" when it carries leaf refs; a
          # fold with no --key-index-leaves over such a HEAD reports
          # key_index_switch_off_over_leaves true (fold.rs:401-410), as does
          # every fold with STUB_SWITCH_OFF_OVER_LEAVES.
          *" catalog fold "*)
            printf 'docker-state fold ravel-server=%s\n' "$(cat "${STUB_STATE}.ravel" 2>/dev/null || echo running)" >>"${STUB_LOG}"
            [ "${STUB_FOLD_EXIT:-0}" -eq 0 ] || exit "${STUB_FOLD_EXIT}"
            case " $* " in
              *" --rebuild-key-index "*)
                [ "${STUB_STAGE1_LEAVES_EXIT:-0}" -eq 0 ] || exit "${STUB_STAGE1_LEAVES_EXIT}"
                : >"${STUB_STATE}.rebuilt"
                echo head-rebuilt-leaves >"${STUB_STATE}.head"
                foldreport rebuild "{\"leaves_written\": ${STUB_STAGE1_LEAVES:-12}, \"parts_rebuilt\": ${STUB_STAGE1_PARTS_REBUILT:-3}}"
                exit 0
                ;;
              *" --json "*)
                [ "${STUB_STAGE1_FOLD_EXIT:-0}" -eq 0 ] || exit "${STUB_STAGE1_FOLD_EXIT}"
                n=$(($(cat "${STUB_STATE}.folds" 2>/dev/null || echo 0) + 1))
                echo "$n" >"${STUB_STATE}.folds"
                prev_head="$(cat "${STUB_STATE}.head" 2>/dev/null || echo head-0)"
                switch_off=false leafy=""
                case " $* " in
                  *" --key-index-leaves "*) leafy=-leaves ;;
                  *) case "$prev_head" in *leaves*) switch_off=true ;; esac ;;
                esac
                [ -z "${STUB_SWITCH_OFF_OVER_LEAVES:-}" ] || switch_off=true
                echo "head-${n}${leafy}" >"${STUB_STATE}.head"
                if [ ! -e "${STUB_STATE}.extra" ]; then
                  foldreport json "{\"watermark_hour\": 490001, \"previous_watermark_hour\": 490001, \"no_op\": true, \"list_requests\": 4, \"get_requests\": 6, \"key_index_switch_off_over_leaves\": ${switch_off}}"
                else
                  leaves=0 bytes=0
                  [ -z "$leafy" ] || { leaves=2 bytes="${STUB_EXTRA_LEAF_BYTES:-10}"; }
                  foldreport json "{\"watermark_hour\": 490002, \"previous_watermark_hour\": 490001, \"entry_count\": 30, \"buckets_folded\": 4, \"list_requests\": 4, \"get_requests\": 6, \"put_requests\": 3, \"key_index_leaves_written\": ${leaves}, \"key_index_leaf_bytes_written\": ${bytes}, \"key_index_switch_off_over_leaves\": ${switch_off}}"
                fi
                exit 0
                ;;
            esac
            hour=$(($(date -u +%s) / 3600))
            seal="${STUB_FOLD_SEAL:-Some(${hour})}"
            case "$seal" in Some\(*\)) seal="${seal#Some(}" seal="${seal%)}" ;; *) seal=null ;; esac
            foldreport text "{\"watermark_hour\": $((hour - ${STUB_FOLD_WATERMARK_LAG:-0})), \"buckets_folded\": 2, \"entry_count\": 420, \"part_bytes\": 9000, \"seal_through_hour\": ${seal}}"
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

# foldreport MODE OVERRIDES: a catalog fold report as ravel-cli prints it,
# every field at its zero value but the JSON object OVERRIDES. MODE json is
# `catalog fold --json`, text the plain fold, rebuild `catalog fold
# --rebuild-key-index --json`; all on stdout, nothing on stderr.
cat >"${BIN}/foldreport" <<'STUB'
#!/usr/bin/env python3
import json
import sys

mode, overrides = sys.argv[1], json.loads(sys.argv[2])
if mode == "rebuild":
    # crates/ravel-catalog/src/key_index_rebuild.rs:83-138, every field.
    leaves = overrides.get("leaves_written", 0)
    doc = {"largest_unindexed_part_entries": 10, "fields": ["label_value"], "parts_unindexed": 3,
           "parts_with_refused_leaves": 0, "leaf_prefix_gets": 2, "leaf_prefix_bytes_read": 512,
           "parts_rebuilt": 0, "parts_deferred": 0, "leaves_written": 0, "leaf_bytes_written": 4096,
           "section_bytes_read": 2048, "ranged_gets": 7, "uncovered_entries": 0, "parts_over_cap": 0,
           "parts_erasure_gated": 0, "erasure_list_requests": 1, "erasure_get_requests": 1,
           "attempt_count": 2,
           # key_index_rebuild.rs:65-80, every field.
           "attempts": [{"parts": 2, "part_gets": 2, "ranged_gets": 3, "leaf_puts": 8, "head_cas_tries": 2, "parts_published": 0},
                        {"parts": 3, "part_gets": 3, "ranged_gets": 4, "leaf_puts": leaves, "head_cas_tries": 1, "parts_published": 3}]}
    doc.update(overrides)
    # services/ravel-cli/src/catalog.rs:722-746: the report plus store and
    # signal, serde_json pretty (no preserve_order, so keys sorted).
    doc.update({"store": "s3", "signal": "metrics"})
    print(json.dumps(doc, indent=2, sort_keys=True))
    sys.exit(0)

# crates/ravel-catalog/src/fold.rs:179-442 FoldReport, every field in
# declaration order.
doc = {"watermark_hour": None, "previous_watermark_hour": None, "no_op": False, "rebuilt": False,
       "buckets_folded": 0, "entry_count": 0, "part_bytes": 0, "parts_total": 0, "parts_reused": 0,
       "list_requests": 0, "get_requests": 0, "put_requests": 0, "postings_built": False,
       "postings_bytes": 0, "column_stats_part_objects_built": 0, "column_stats_dictionaries_dropped": 0,
       "layout_drift_count": 0, "frontier_hours_reconciled": 0, "frontier_hours_deferred": 0,
       "refold_hours_reconciled": 0, "stamped_records": 0, "stamped_entries": 0,
       "seal_through_hour": None, "key_index_ranged_gets": 0, "key_index_bytes_read": 0,
       "key_index_leaves_written": 0, "key_index_leaf_bytes_written": 0, "key_index_uncovered_entries": 0,
       "key_index_sections_read": 0, "key_index_entries_carried": 0, "key_index_build_passes": 0,
       "key_index_pair_budget": 0, "key_index_field_pairs": {}, "key_index_switch_off_over_leaves": False,
       "key_index_parts_missing_declared_leaves": 0, "key_index_leaf_gets": 0,
       "metrics_label_index_parts_over_cap": 0, "metrics_label_index_parts_erasure_gated": 0,
       "metrics_label_index_erasure_list_requests": 0}
doc.update(overrides)
if mode == "json":
    # catalog.rs:428-489: the report plus store, signal, seal_margin and
    # snapshot_part_max_entries, serde_json pretty with keys sorted.
    doc.update({"store": "s3", "signal": "metrics", "seal_margin": "1h 20m",
                "snapshot_part_max_entries": 250000})
    print(json.dumps(doc, indent=2, sort_keys=True))
    sys.exit(0)

# catalog.rs:494-617, the text report line by line: Option as {:?}, bools
# lowercase, key_index_field_pairs as compact JSON.
def show(v):
    if isinstance(v, bool):
        return str(v).lower()
    return str(v)
def opt(v):
    return "None" if v is None else f"Some({v})"
print("store: s3")
print("signal: metrics")
print(f"no_op: {show(doc['no_op'])}")
print(f"rebuilt: {show(doc['rebuilt'])}")
print(f"previous_watermark_hour: {opt(doc['previous_watermark_hour'])}")
print(f"watermark_hour: {opt(doc['watermark_hour'])}")
print("seal_margin: 1h 20m")
print(f"seal_through_hour: {opt(doc['seal_through_hour'])}")
print("snapshot_part_max_entries: 250000")
for k in ["buckets_folded", "entry_count", "part_bytes", "parts_total", "parts_reused", "list_requests",
          "get_requests", "put_requests", "postings_built", "postings_bytes",
          "column_stats_part_objects_built", "column_stats_dictionaries_dropped", "layout_drift_count",
          "frontier_hours_reconciled", "frontier_hours_deferred", "refold_hours_reconciled",
          "stamped_records", "stamped_entries", "key_index_ranged_gets", "key_index_bytes_read",
          "key_index_leaves_written", "key_index_leaf_bytes_written", "key_index_uncovered_entries",
          "key_index_sections_read", "key_index_entries_carried", "key_index_build_passes",
          "key_index_pair_budget"]:
    print(f"{k}: {show(doc[k])}")
print("key_index_field_pairs: " + json.dumps(doc["key_index_field_pairs"], separators=(",", ":"), sort_keys=True))
for k in ["key_index_switch_off_over_leaves", "key_index_parts_missing_declared_leaves", "key_index_leaf_gets",
          "metrics_label_index_parts_over_cap", "metrics_label_index_parts_erasure_gated",
          "metrics_label_index_erasure_list_requests"]:
    print(f"{k}: {show(doc[k])}")
STUB

# date: the host's own, except `date -u +%s` once $STUB_STATE.clock exists
# (the erase submit stub writes STUB_CLOCK_AT_SUBMIT there), which prints
# that instant.
cat >"${BIN}/date" <<'STUB'
#!/usr/bin/env bash
if [ "$*" = "-u +%s" ] && [ -s "${STUB_STATE}.clock" ]; then
  cat "${STUB_STATE}.clock"
  exit 0
fi
exec "${STUB_REAL_DATE}" "$@"
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
    # STUB_METRICS_DOWN_FROM on, and at scrape STUB_METRICS_DOWN_AT alone, it
    # answers 503.
    k=$(($(cat "${STUB_STATE}.scrapes" 2>/dev/null || echo 0) + 1))
    echo "$k" >"${STUB_STATE}.scrapes"
    if { [ -n "${STUB_METRICS_DOWN_FROM:-}" ] && [ "$k" -ge "${STUB_METRICS_DOWN_FROM}" ]; } ||
      [ "${STUB_METRICS_DOWN_AT:-0}" = "$k" ]; then
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
  *:4318/api/v1/status/buildinfo)
    if [ -e "${STUB_STATE}.notready" ]; then printf 503; else printf 200; fi
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

# python3 (in PYFAIL_DIR, ahead of everything on PATH only while STUB_PY_OP is
# set): run.sh's py OP STAMPS KEY ... call whose OP is STUB_PY_OP and KEY is
# STUB_PY_KEY exits 1 writing and printing nothing, as a py set or py get that
# failed would, or with STUB_PY_PRINT set prints that and exits 0; every other
# call runs the real python3.
PYFAIL_DIR="${TMP}/pyfail"
mkdir -p "${PYFAIL_DIR}"
cat >"${PYFAIL_DIR}/python3" <<STUB
#!/usr/bin/env bash
if [ "\$1 \$2" = "-I -c" ] && [ "\${4:-}" = "\${STUB_PY_OP}" ] && [ "\${6:-}" = "\${STUB_PY_KEY}" ]; then
  printf 'python3 %s %s %s: stub\n' "\$4" "\$5" "\$6" >>"\${STUB_LOG}"
  if [ -n "\${STUB_PY_PRINT:-}" ]; then
    printf '%s\n' "\${STUB_PY_PRINT}"
    exit 0
  fi
  exit 1
fi
exec $(command -v python3) "\$@"
STUB
chmod +x "${PYFAIL_DIR}/python3"

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
# --steps: the Stage 1 extra hour, after which the catalog stub lists and
# folds an ingest hour 490002; exit STUB_EXTRA_INGEST_EXIT.
case " $* " in
  *" --steps "*)
    [ "${STUB_EXTRA_INGEST_EXIT:-0}" -eq 0 ] || exit "${STUB_EXTRA_INGEST_EXIT}"
    : >"${STUB_STATE}.extra"
    ;;
esac
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
# The report carries timed_rows, the entries of the --corpus it was given, as
# the stand-in for the timed denominators; --diagnostics gets one ravel_cost
# record per entry, with the five phases, as the real sidecar does. A call
# whose --out is under cold-ravel-diagnostic/ exits STUB_COLD_QUERY_EXIT.
corpus=""; diag=""; out=""
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --corpus) corpus="${args[$((i + 1))]}" ;;
    --diagnostics) diag="${args[$((i + 1))]}" ;;
    --out) out="${args[$((i + 1))]}" ;;
  esac
done
case "$out" in */cold-ravel-diagnostic/*) [ "${STUB_COLD_QUERY_EXIT:-0}" -eq 0 ] || exit "${STUB_COLD_QUERY_EXIT}" ;; esac
# STUB_COLD_DROP_RECORD=ID.R: the cold run of entry ID, repetition R, blocks
# the runs file's temporary path so the record after it cannot be written;
# the next cold run clears it, so exactly that one record goes missing.
case "$out" in
  */cold-ravel-diagnostic/*)
    if [ -n "${STUB_COLD_DROP_RECORD:-}" ] && [ "${out##*/}" = "${STUB_COLD_DROP_RECORD}.report.json" ]; then
      mkdir -p "${out%/*}/runs.json.tmp"
    else
      rmdir "${out%/*}/runs.json.tmp" 2>/dev/null
    fi
    ;;
esac
# Each record also carries the Stage 1 fields: --pass-label and --fold-state
# as given (null without), result_hash blake3:<entry> (blake3:<entry>-routed
# for entry STUB_ROUTED_HASH_DIFF under folded-routed),
# segmentsFetched 4 and segmentsPruned 6, and segmentsPrunedByLabelIndex 3
# under folded-routed, else 0. Under folded-routed, entry
# STUB_ROUTED_FAILED_ENTRY's record failed: no result_hash, no segment
# figures, status "failed: stub". Under folded-unrouted, entry
# STUB_UNROUTED_PRUNED_ENTRY's record says segmentsPrunedByLabelIndex 2.
# Entry STUB_INCORRECT_ENTRY's records say status incorrect in every pass,
# and entry STUB_ROUTED_STATUS_ENTRY's under folded-routed only, each with its
# result_hash. Entry STUB_ABSENT_ENTRY's records say segmentsFetched 0 and
# segmentsPruned 10, a metric no object holds, and under folded-routed
# segmentsPrunedByLabelIndex 0. Under folded-routed, each
# record's phases end with a keyIndex phase in the server's shape, 0 GETs and
# STUB_ROUTED_KEYINDEX_HITS (default 2) cache hits; entry
# STUB_ROUTED_LEAF_UNREAD_ENTRY's shows 0 of both, and with
# STUB_ROUTED_PRUNES set every record's segmentsPrunedByLabelIndex is that.
# A folded-routed record's labelIndexValues holds one __name__ pass: no leaf
# GET, the keyIndex phase's cache hits, 3 parts covered, every prune its own;
# any other record's is null.
# STUB_RUN_FIGURES names a JSON file whose "records" object maps a pass label
# to entry ids ("*" for every entry) to the fields that replace the record's;
# a "keyIndex" field there replaces the routed keyIndex phase's figures. Its
# "rep2" object, keyed the same way, adds a rep-2 record per entry to that
# pass with those fields replaced.
# Every phase also carries the per-phase cost the server reports: 1 GET of
# 100 wire bytes.
# Entry STUB_TWO_HASH_ENTRY gets a second record in every pass, rep 2, with
# result_hash blake3:<entry>-rep2.
python3 -I - "$corpus" "$diag" "$out" "$verdict" "$@" <<'PY'
import json, os, sys
corpus, diag, out, verdict = sys.argv[1:5]
rest = sys.argv[5:]
def flag(name):
    return rest[rest.index(name) + 1] if name in rest else None
label, state = flag("--pass-label"), flag("--fold-state")
routed = label == "folded-routed"
differ = os.environ.get("STUB_ROUTED_HASH_DIFF", "")
failed = os.environ.get("STUB_ROUTED_FAILED_ENTRY", "") if routed else ""
unrouted_pruned = os.environ.get("STUB_UNROUTED_PRUNED_ENTRY", "") if label == "folded-unrouted" else ""
ids = [e["id"] for e in json.load(open(corpus))["entries"]]
phases = [{"name": n, "requests": 1, "phase": n, "s3GetRequests": 1, "s3GetWireBytes": 100}
          for n in ("resolve", "plan", "probe", "scan", "decode")]
routed_prunes = int(os.environ.get("STUB_ROUTED_PRUNES") or 3)
def key_index(i):
    hits = 0 if i == os.environ.get("STUB_ROUTED_LEAF_UNREAD_ENTRY") else int(os.environ.get("STUB_ROUTED_KEYINDEX_HITS") or 2)
    return [{"phase": "keyIndex", "s3GetRequests": 0, "cacheHits": hits}]
def values(i):
    hits = key_index(i)[0]["cacheHits"]
    return [{"labels": ["__name__"], "valueKey8": "00112233445566ff", "leafGetRequests": 0,
             "leafWireBytes": 0, "leafCacheHits": hits, "partsCovered": 3, "partsUncovered": 0,
             "partsBeyondCeiling": 0, "leavesRejected": 0, "reservationsRefused": 0,
             "skippedByBudget": 0, "segmentsPruned": routed_prunes,
             "segmentsPrunedAlone": routed_prunes}]
def record(i):
    r = {"entry": i, "pass": 1, "rep": 1, "status": "ok", "latency_ms": 12.5,
         "accounting": {"requests": 5}, "phases": phases + key_index(i) if routed else phases,
         "segmentsFetched": 4, "segmentsPruned": 6, "segmentsPrunedByDictionary": 3,
         "segmentsPrunedByLabelIndex": routed_prunes if routed else (2 if i == unrouted_pruned else 0),
         "labelIndexValues": values(i) if routed else None,
         "estimate": None, "io": None, "pass_label": label, "fold_state": state,
         "result_hash": f"blake3:{i}-routed" if routed and i == differ else f"blake3:{i}"}
    if i == os.environ.get("STUB_INCORRECT_ENTRY") or (routed and i == os.environ.get("STUB_ROUTED_STATUS_ENTRY")):
        r["status"] = "incorrect"
    if i == os.environ.get("STUB_ABSENT_ENTRY"):
        r.update({"segmentsFetched": 0, "segmentsPruned": 10})
        if routed:
            r["segmentsPrunedByLabelIndex"] = 0
    if i == failed:
        r.update({"status": "failed: stub", "result_hash": None, "segmentsFetched": None,
                  "segmentsPruned": None, "segmentsPrunedByLabelIndex": None, "labelIndexValues": None})
    if os.environ.get("STUB_RUN_FIGURES"):
        figs = json.load(open(os.environ["STUB_RUN_FIGURES"]))["records"].get(label) or {}
        over = {**figs.get("*", {}), **figs.get(i, {})}
        key_figs = over.pop("keyIndex", None)
        r.update(over)
        if routed and key_figs is not None:
            r["phases"] = phases + [{"phase": "keyIndex", **key_figs}]
    return r
if diag:
    rows = [record(i) for i in ids]
    two = os.environ.get("STUB_TWO_HASH_ENTRY")
    if two in ids:
        rows.append({**record(two), "rep": 2, "result_hash": f"blake3:{two}-rep2"})
    if os.environ.get("STUB_RUN_FIGURES"):
        rep2 = (json.load(open(os.environ["STUB_RUN_FIGURES"])).get("rep2") or {}).get(label) or {}
        rows += [{**record(i), "rep": 2, **rep2.get("*", {}), **rep2.get(i, {})}
                 for i in ids if rep2]
    json.dump({"ravel_cost": rows}, open(diag, "w"))
if out:
    report = {"stub": "report", "timed_rows": len(ids)}
    if verdict:
        report["run_verdict"] = "failed"
    json.dump(report, open(out, "w"))
PY
# STUB_CORPUS_CLOBBER: the standard run leaves its corpus file unreadable for
# the cold pass after it.
case "$out" in
  */cold-ravel-diagnostic/*) ;;
  *) [ -z "${STUB_CORPUS_CLOBBER:-}" ] || printf 'not json\n' >"$corpus" ;;
esac
exit 0
STUB

# metricsbench_holders: logs how many of the keys --objects-list names are
# present under --objects-dir, then writes --out with one statement per corpus
# entry: 10 segments in its window, 4 of them holding its one metric (so
# unrouted_fetched_expected 4, the stub query records' segmentsFetched; their
# fetched + pruned is 10), no token-only object, two holders, route-bound,
# one value holder, so expected_routed_prunes 3, the stub routed records'
# segmentsPrunedByLabelIndex. It reports one commit token naming one object.
# Entry STUB_HOLDERS_NAMED_SPLIT gets 7 in its window, 4 named, so fetched +
# pruned is not segments_in_window; entry STUB_ABSENT_ENTRY 7 in its window, 0
# named, 0 holders and 0 value holders; STUB_HOLDERS_WINDOW_OFF 5 named and
# expected; STUB_HOLDERS_WINDOW_NULL null counts; STUB_HOLDERS_UNBOUND is not
# route-bound and carries no value holders; STUB_HOLDERS_PRUNES_OFF expects 2
# routed prunes. STUB_RUN_FIGURES names a JSON file whose "holders" object
# holds "report", the top-level fields to replace, and "statements", entry ids
# ("*" for every entry) to the fields to replace. Exit STUB_HOLDERS_EXIT
# writes nothing.
cat >"${BIN}/metricsbench_holders" <<'STUB'
#!/usr/bin/env bash
printf 'metricsbench_holders %s\n' "$*" >>"${STUB_LOG}"
[ "${STUB_HOLDERS_EXIT:-0}" -eq 0 ] || exit "${STUB_HOLDERS_EXIT}"
args=("$@")
corpus=""; objects=""; out=""; listing=""; ingest=""
for ((i = 0; i < ${#args[@]}; i++)); do
  case "${args[$i]}" in
    --corpus) corpus="${args[$((i + 1))]}" ;;
    --objects-dir) objects="${args[$((i + 1))]}" ;;
    --objects-list) listing="${args[$((i + 1))]}" ;;
    --ingest-report) ingest="${args[$((i + 1))]}" ;;
    --out) out="${args[$((i + 1))]}" ;;
  esac
done
python3 -I - "$corpus" "$out" "$ingest" "$listing" "$objects" <<'PY'
import json, os, sys
corpus, out, ingest, listing, objects = sys.argv[1:]
listed = [json.loads(l) for l in open(listing).read().splitlines() if l.strip()]
present = sum(1 for o in listed if os.path.isfile(os.path.join(objects, o["key"])))
with open(os.environ["STUB_LOG"], "a") as log:
    log.write(f"metricsbench_holders saw {present} of {len(listed)} listed objects\n")
env = lambda k: os.environ.get(k, "")
ids = [e["id"] for e in json.load(open(corpus))["entries"]]
def statement(i):
    s = {"id": i, "promql": "up", "selectors": [], "positive_equalities": [], "metrics": ["up"],
         "name_pruned": True, "segments_in_window": 10, "named_in_window": 4, "token_only_fetched": 0,
         "unrouted_fetched_expected": 4, "holders": 2, "value_holders": 1, "expected_routed_prunes": 3,
         "route_bound": True, "route_bound_reason": None}
    if i == env("STUB_HOLDERS_NAMED_SPLIT"):
        s["segments_in_window"] = 7
    if i == env("STUB_ABSENT_ENTRY"):
        s.update({"segments_in_window": 7, "named_in_window": 0, "unrouted_fetched_expected": 0, "holders": 0,
                  "value_holders": 0, "expected_routed_prunes": 0})
    if i == env("STUB_HOLDERS_WINDOW_OFF"):
        s.update({"named_in_window": 5, "unrouted_fetched_expected": 5})
    if i == env("STUB_HOLDERS_WINDOW_NULL"):
        s.update({"segments_in_window": None, "named_in_window": None, "unrouted_fetched_expected": None})
    if i == env("STUB_HOLDERS_UNBOUND"):
        s.update({"route_bound": False, "route_bound_reason": "stub: no qualifying matcher",
                  "value_holders": None, "expected_routed_prunes": None})
    if i == env("STUB_HOLDERS_PRUNES_OFF"):
        s.update({"value_holders": 2, "expected_routed_prunes": 2})
    if figs:
        s.update(figs["statements"].get("*", {}))
        s.update(figs["statements"].get(i, {}))
    return s
figs = json.load(open(env("STUB_RUN_FIGURES")))["holders"] if env("STUB_RUN_FIGURES") else None
report = {"method": "stub method", "profile": json.load(open(ingest))["profile"]["name"],
          "objects_l0": sum(1 for o in listed if o["level"] == "L0"),
          "objects_l1": sum(1 for o in listed if o["level"] == "L1"), "series_total": 40,
          "min_commit_tokens": 1, "token_objects": ["t/stub/token-object"], "tokens_unmatched": [],
          "statements": [statement(i) for i in ids],
          "skipped": [{"id": "skipped_one", "reason": "no positive equality"}]}
if figs:
    report.update(figs["report"])
json.dump(report, open(out, "w"))
PY
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
# METRICSBENCH_SLEEP: the Stage 1 arms' waits, logged and not slept.
cat >"${BIN}/stubsleep" <<'STUB'
#!/usr/bin/env bash
printf 'stubsleep %s\n' "$*" >>"${STUB_LOG}"
STUB
chmod +x "${BIN}/stubsleep"

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
  [ -z "${STUB_PY_OP:-}" ] || run_path="${PYFAIL_DIR}:${run_path}"
  PATH="${run_path}" \
    STUB_LOG="${LOG}" STUB_STATE="${TMP}/${name}/state" STUB_REAL_UNAME="${REAL_UNAME}" STUB_REAL_DATE="${REAL_DATE}" \
    METRICSBENCH_S3S_FS_CACHE="${TMP}/${name}/cache" \
    METRICSBENCH_CAPTURE="${BIN}/capture" \
    METRICSBENCH_SAMPLE_INTERVAL=0.1 \
    METRICSBENCH_READY_TRIES=3 METRICSBENCH_READY_INTERVAL=0 \
    METRICSBENCH_IMDS="http://imds.invalid" METRICSBENCH_SLEEP="${BIN}/stubsleep" \
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
# Without MB_STAGE1, ravel.yml's entrypoint gets an empty RAVEL_SERVER_EXTRA_FLAGS
# (see case z0 for the rendered command line) and no Stage 1 step runs.
if grep -qx 'docker-env up RAVEL_SERVER_EXTRA_FLAGS=' "${LOG}"; then
  pass "a: compose up gets an empty RAVEL_SERVER_EXTRA_FLAGS"
else
  fail "a: compose up's RAVEL_SERVER_EXTRA_FLAGS is not empty: $(grep 'RAVEL_SERVER_EXTRA_FLAGS' "${LOG}")"
fi
check "a: stage1 stamped off" 'j("stamps.json")["stage1"] == {"enabled": False}'
expect_no_log "a: no ravel-server recreate" "--force-recreate"
expect_no_log "a: no Stage 1 fold" "--signal metrics --json"
expect_no_log "a: no leaf rebuild" "--rebuild-key-index"
expect_no_log "a: no holders count" "metricsbench_holders"
expect_no_log "a: the provenance is not marked non-comparable" "--config comparable=false"
expect_log "a: the query step's records are labelled with the fold gauge it ran against" "--pass-label background-folded --fold-state ravel_catalog_fold_last_success_timestamp_seconds=1761000000"
check "a: the query step's ravel_cost records carry that label" 'all(r["pass_label"] == "background-folded" and r["fold_state"] == "ravel_catalog_fold_last_success_timestamp_seconds=1761000000" for r in j("diagnostics.json")["ravel_cost"])'

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
expect_log "j: with no fold gauge the query step's records are labelled unfolded" "--pass-label unfolded --fold-state absent: no ravel_catalog_fold_last_success_timestamp_seconds"

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

# --- o2: the Go comparators' memory limit (ADR-0927 amendment 2026-11-05) -----
# Same block rule as o: the service's environment mapping must carry
# GOMEMLIMIT: 7GiB, and nowhere in the block may GOGC be set.
for svc in prometheus victoriametrics mimir; do
  if python3 -I - "${HERE}/../docker-compose.yml" "${svc}" <<'PY'; then
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
env = next((i for i, l in enumerate(block) if l == "    environment:"), None)
if env is None:
    sys.exit(1)
entries = []
for l in block[env + 1:]:
    if not l.startswith("      ") or l.strip().startswith("#"):
        if l.strip().startswith("#"):
            continue
        break
    entries.append(l.strip())
ok = "GOMEMLIMIT: 7GiB" in entries and not any(e.startswith("GOGC") for e in entries)
sys.exit(0 if ok else 1)
PY
    pass "o2: ${svc} in docker-compose.yml carries GOMEMLIMIT 7GiB and no GOGC"
  else
    fail "o2: ${svc} in docker-compose.yml lacks environment GOMEMLIMIT: 7GiB, or sets GOGC"
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
expect_log "u: the folded read-back records each query's cost from Ravel's /metrics" "--readback-settle-secs 60 --ravel-metrics-url http://127.0.0.1:4318/metrics"
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

# --- x: the object event-time spread file -----------------------------------------
OUT="${TMP}/a/out"
LOG="${TMP}/a/calls.log"
expect_file x ravel-object-spans.json
check "x: the five buckets in order, 60 objects and 6000 bytes each" '[(b["bucket"], b["objects"], b["bytes"]) for b in j("ravel-object-spans.json")["histogram"]] == [("under 1 h",60,6000),("1 to 2 h",60,6000),("2 to 6 h",60,6000),("6 to 24 h",60,6000),("over 24 h",60,6000)]'
check "x: the buckets sum exactly to ravel-objects.json's data count" 'sum(b["objects"] for b in j("ravel-object-spans.json")["histogram"]) == j("ravel-objects.json")["data"]["count"] == j("ravel-object-spans.json")["object_count"] == 300 and sum(b["bytes"] for b in j("ravel-object-spans.json")["histogram"]) == j("ravel-objects.json")["data"]["bytes"]'
check "x: distinct ingest hours and covering snapshot parts" 'j("ravel-object-spans.json")["distinct_ingest_hours"] == 2 and j("ravel-object-spans.json")["snapshot_parts"] == 3 and j("ravel-object-spans.json")["matches_ravel_objects"] is True'
check "x: the stamp names both inspector commands and the file" 'j("stamps.json")["object_spans"]["status"] == "ok" and j("stamps.json")["object_spans"]["file"] == "ravel-object-spans.json" and "ravel-cli catalog list --tenant metricsbench" in j("stamps.json")["object_spans"]["inspector"]["entries"] and j("stamps.json")["object_spans"]["inspector"]["snapshot_parts"] == "ravel-cli catalog inspect --tenant metricsbench --signal metrics" and j("stamps.json")["object_spans"]["matches_ravel_objects"] is True'
if python3 -I - "${LOG}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
fold = next(i for i, l in enumerate(lines) if l.startswith("docker-state fold "))
listed = next(i for i, l in enumerate(lines) if l.startswith("docker-state catalog-list "))
sys.exit(0 if fold < listed else 1)
PY
  pass "x: the entries are read after the fold"
else
  fail "x: the entries are not read after the fold"
fi
expect_log "x: the entries listing asks for JSON" "catalog list --tenant metricsbench --shards 4 --hours"
check "x: a ci run selects no cold entry, stamped with the corpus path" 'j("stamps.json")["cold_ravel_diagnostic"] == "not applicable: no entry of class long_range or mb_full_range_avg_over_time in the corpus " + j("stamps.json")["corpus"] + " is declared for profile ci" and j("stamps.json")["corpus"].endswith("benchmarks/metrics/metrics.corpus.json")'
expect_no_file x cold-ravel-diagnostic.json
expect_no_log "x: a ci run restarts nothing for a cold pass" " restart --no-deps ravel-server"

mapfile -t args < <(full_args "${TMP}/x0/out")
STUB_LIST_DATA=7 run_case x0 "${args[@]}"
expect_rc x0 0
check "x0: 7 listed objects give 7 in the buckets and in ravel-objects.json, not 300" 'sum(b["objects"] for b in j("ravel-object-spans.json")["histogram"]) == 7 == j("ravel-objects.json")["data"]["count"] and [b["objects"] for b in j("ravel-object-spans.json")["histogram"]] == [2,2,1,1,1]'

mapfile -t args < <(full_args "${TMP}/x1/out")
STUB_INSPECT_HEAD_ABSENT=1 run_case x1 "${args[@]}"
expect_rc x1 0
expect_no_file x1 ravel-object-spans.json
check "x1: no snapshot HEAD is a stamped failure with its reason" 'j("stamps.json")["object_spans"].startswith("failed: no snapshot HEAD") and j("stamps.json")["status"] == "passed"'

mapfile -t args < <(full_args "${TMP}/x2/out")
STUB_SPANS_LIST_EXIT=3 run_case x2 "${args[@]}"
expect_rc x2 0
expect_no_file x2 ravel-object-spans.json
check "x2: a failed inspector is stamped with its command, the run passes" 'j("stamps.json")["object_spans"].startswith("failed: ravel-cli catalog list --tenant metricsbench") and j("stamps.json")["object_spans"].endswith("exited 3") and j("stamps.json")["status"] == "passed"'

mapfile -t args < <(full_args "${TMP}/x3/out")
STUB_INGEST_ROW_FAILURE=ravel run_case x3 "${args[@]}"
expect_no_file x3 ravel-object-spans.json
check "x3: no fold ran, so the capture is stamped failed with the reason" 'j("stamps.json")["object_spans"].startswith("failed: no fold ran: ")'

mapfile -t args < <(full_args "${TMP}/x4/out")
STUB_SPANS_INSPECT_EXIT=2 run_case x4 "${args[@]}"
expect_rc x4 0
expect_no_file x4 ravel-object-spans.json
check "x4: a failed catalog inspect is stamped with its command, the run passes" 'j("stamps.json")["object_spans"] == "failed: ravel-cli catalog inspect --tenant metricsbench --signal metrics exited 2" and j("stamps.json")["status"] == "passed"'

mapfile -t args < <(full_args "${TMP}/x5/out")
STUB_SPANS_L1=1 run_case x5 "${args[@]}"
expect_rc x5 0
if [ "$(grep -c '"level": "L1"' "${OUT}/ravel-object-spans.list.txt")" -eq 100 ]; then pass "x5: the listing carries 100 L1 lines"; else fail "x5: the listing does not carry 100 L1 lines"; fi
check "x5: L1 records are read, the capture succeeds over all 300" 'j("stamps.json")["object_spans"]["status"] == "ok" and j("ravel-object-spans.json")["object_count"] == 300 and sum(b["objects"] for b in j("ravel-object-spans.json")["histogram"]) == 300'

mapfile -t args < <(full_args "${TMP}/x6/out")
STUB_SPANS_HEADER_STDOUT=1 run_case x6 "${args[@]}"
expect_rc x6 0
expect_no_file x6 ravel-object-spans.json
check "x6: a non-JSON line on the listing stdout is a stamped failure" 'j("stamps.json")["object_spans"] == "failed: line 1 of the catalog listing is not a JSON document" and j("stamps.json")["status"] == "passed"'

mapfile -t args < <(full_args "${TMP}/x7/out")
STUB_SPANS_OBJECTS_GONE="${TMP}/x7/out/ravel-objects.json" run_case x7 "${args[@]}"
expect_rc x7 0
check "x7: no ravel-objects.json to compare against is null, not false" 'j("ravel-object-spans.json")["ravel_objects_data_count"] is None and j("ravel-object-spans.json")["matches_ravel_objects"] is None and j("stamps.json")["object_spans"]["matches_ravel_objects"] is None'

OUT="${TMP}/f/out"
check "f: the diagnostic mode stamps both captures not applicable" 'j("stamps.json")["object_spans"] == "not applicable: diagnostic run starts no Ravel" and j("stamps.json")["cold_ravel_diagnostic"] == "not applicable: diagnostic run starts no Ravel"'

# --- y: the Ravel-only cold diagnostic pass ------------------------------------------
# A corpus whose ids are not the real corpus's: the selection must follow the
# file. Expected for history: both long_range entries declared for it (one with
# no profiles list) and the named full-range entry; not the long_range entry
# declared for churn only, not the other full-range entry, not the rest.
python3 -I - "${TMP}/corpus.json" <<'PY'
import json, sys
def e(i, cls, profiles=None):
    d = {"id": i, "class": cls, "promql": "up", "eval": "range"}
    if profiles is not None:
        d["profiles"] = profiles
    return d
json.dump({"version": 1, "entries": [
    e("zz_long_history", "long_range", ["history"]),
    e("zz_long_any", "long_range"),
    e("zz_long_churn_only", "long_range", ["churn"]),
    e("mb_full_range_avg_over_time", "full_range", ["cardinality", "history", "churn"]),
    e("zz_other_full_range", "full_range", ["history"]),
    e("zz_single", "single_series", ["history"]),
]}, open(sys.argv[1], "w"))
PY
COLD_WANT='["zz_long_history", "zz_long_any", "mb_full_range_avg_over_time"]'
run_cold() {
  local name="$1"
  shift
  OUT="${TMP}/${name}/out"
  run_case "$name" --profile history --ravel-image "${IMAGE}" --ravel-source-commit "${COMMIT}" \
    --bin-dir "${BIN}" --out-dir "${OUT}" --anchor-ms "${ANCHOR}" "$@"
}
METRICSBENCH_CORPUS="${TMP}/corpus.json" run_cold y
expect_rc y 0
expect_file y cold-ravel-diagnostic.json
check "y: the selected entries are exactly the corpus's long_range class plus the full-range entry for the profile" 'j("cold-ravel-diagnostic.json")["entries"] == '"${COLD_WANT}"' and sorted({r["entry"] for r in j("cold-ravel-diagnostic.json")["runs"]}) == sorted('"${COLD_WANT}"')'
check "y: five repetitions per selected entry, numbered 1 to 5" 'all(sorted(r["repetition"] for r in j("cold-ravel-diagnostic.json")["runs"] if r["entry"] == i) == [1,2,3,4,5] for i in '"${COLD_WANT}"') and len(j("cold-ravel-diagnostic.json")["runs"]) == 15'
check "y: every run carries status, latency and the ravel_cost record with exactly the five phases" 'all(r["status"] == "ok" and r["latency_ms"] == 12.5 and [p["name"] for p in r["ravel_cost"]["phases"]] == ["resolve","plan","probe","scan","decode"] and r["ravel_cost"]["accounting"] == {"requests": 5} for r in j("cold-ravel-diagnostic.json")["runs"])'
check "y: the file names the regime and marks it non-comparable" 'j("cold-ravel-diagnostic.json")["regime"] == "cold-ravel-diagnostic" and j("cold-ravel-diagnostic.json")["comparable"] is False and "not comparable" in j("cold-ravel-diagnostic.json")["note"] and "#3794" in j("cold-ravel-diagnostic.json")["note"]'
check "y: the stamp names the regime, non-comparable, with no failed run" 'j("stamps.json")["cold_ravel_diagnostic"]["regime"] == "cold-ravel-diagnostic" and j("stamps.json")["cold_ravel_diagnostic"]["comparable"] is False and j("stamps.json")["cold_ravel_diagnostic"]["failed_runs"] == 0 and j("stamps.json")["cold_ravel_diagnostic"]["runs"] == 15 and j("stamps.json")["status"] == "passed"'
check "y: report.json's timed rows are the standard run's (the whole corpus), not touched by the pass" 'j("report.json") == {"stub": "report", "timed_rows": 6}'
if [ "$(grep -c ' restart --no-deps ravel-server$' "${LOG}")" -eq 15 ]; then pass "y: ravel-server restarted once per run"; else fail "y: restarts $(grep -c ' restart --no-deps ravel-server$' "${LOG}"), want 15"; fi
if [ "$(grep -c '^metricsbench_query --ingest-report' "${LOG}")" -eq 16 ] &&
  [ "$(grep '^metricsbench_query --ingest-report' "${LOG}" | grep -c -- "--out ${OUT}/report.json")" -eq 1 ] &&
  [ "$(grep '^metricsbench_query --ingest-report' "${LOG}" | grep -c -- "--out ${OUT}/cold-ravel-diagnostic/")" -eq 15 ]; then
  pass "y: one standard query run writes report.json, the 15 cold runs write only under cold-ravel-diagnostic/"
else
  fail "y: the query calls do not split into 1 report.json and 15 cold-ravel-diagnostic/ runs"
fi
if grep '^metricsbench_query --ingest-report' "${LOG}" | sed -n '2,$p' | grep -qF -- "--engine name=ravel,url=http://127.0.0.1:4318,bearer_env=METRICSBENCH_RAVEL_TOKEN" &&
  ! grep '^metricsbench_query --ingest-report' "${LOG}" | sed -n '2,$p' | grep -qE -- 'name=(victoriametrics|mimir)'; then
  pass "y: a cold run queries Ravel and the oracle only, one pass of one repetition"
else
  fail "y: a cold run's engines are not Ravel and the oracle"
fi
expect_log "y: a cold run is one untimed-warmup-free pass" "--warmup-passes 0 --passes 1 --reps-per-pass 1"
if python3 -I - "${LOG}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
validate = next(i for i, l in enumerate(lines) if l.startswith("metricsbench_report validate "))
restart = next(i for i, l in enumerate(lines) if l.endswith(" restart --no-deps ravel-server"))
stop = next(i for i, l in enumerate(lines) if l.endswith(" stop ravel-server"))
# The memory step's docker inspect of the stack's containers, after validate.
memory = [i for i, l in enumerate(lines) if l.startswith("docker inspect id-") and i > validate]
# The sampler's last docker stats, which memory_summary takes as it stops it.
stats = [i for i, l in enumerate(lines) if l.startswith("docker stats ")]
sys.exit(0 if memory and validate < memory[0] < restart < stop and stats and stats[-1] < restart else 1)
PY
  pass "y: validate, memory (sampler stopped, containers inspected), then the cold pass, then the fold's stop"
else
  fail "y: the step order is not validate, memory, cold pass, stop"
fi
if python3 -I - "${LOG}" "${OUT}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
cold = "--out " + sys.argv[2] + "/cold-ravel-diagnostic/"
seq = [("restart" if l.endswith(" restart --no-deps ravel-server") else "query")
       for l in lines
       if l.endswith(" restart --no-deps ravel-server") or (l.startswith("metricsbench_query --ingest-report") and cold in l)]
sys.exit(0 if seq == ["restart", "query"] * 15 else 1)
PY
  pass "y: every cold query is preceded by its own restart"
else
  fail "y: the restarts and cold queries do not alternate restart, query, 15 times"
fi
expect_log "y: the cold provenance is built over the single-entry corpus" "--corpus ${OUT}/cold-ravel-diagnostic/corpus-zz_long_any.json"
check "y: the standard steps are unchanged" 'j("stamps.json")["steps_completed"] == ["fetch-s3s-fs","up","qualify","image","ready","sampler","ingest","mimir-readiness","objects","capture","provenance","fold-state","query","validate","memory","stop","fold","start","readback-folded"]'

check "y: the corpus path used is stamped, and named in the file and the stamp" 'j("stamps.json")["corpus"] == "'"${TMP}"'/corpus.json" == j("cold-ravel-diagnostic.json")["corpus"] == j("stamps.json")["cold_ravel_diagnostic"]["corpus"] and j("stamps.json")["cold_ravel_diagnostic"]["missing_runs"] == []'
check "y: the note says each repetition also queries the Prometheus oracle" '"Prometheus oracle" in j("cold-ravel-diagnostic.json")["note"]'

METRICSBENCH_CORPUS="${TMP}/corpus.json" STUB_COLD_QUERY_EXIT=1 run_cold y1
expect_rc y1 0
check "y1: failed cold runs are recorded and stamped, the run passes, report.json untouched" 'j("stamps.json")["status"] == "passed" and j("stamps.json")["cold_ravel_diagnostic"]["failed_runs"] == 15 and all(r["status"] == "failed: metricsbench_query exited 1" and r["ravel_cost"] is None for r in j("cold-ravel-diagnostic.json")["runs"]) and j("report.json") == {"stub": "report", "timed_rows": 6}'

STUB_INGEST_READBACK_FAIL=mimir METRICSBENCH_CORPUS="${TMP}/corpus.json" run_cold y2
expect_rc y2 70
expect_no_file y2 cold-ravel-diagnostic.json
check "y2: after a failed ingest the pass is stamped not applicable" 'j("stamps.json")["cold_ravel_diagnostic"].startswith("not applicable: the ingest verification failed")'
expect_no_log "y2: nothing restarted for it" " restart --no-deps ravel-server"

run_cold y3
expect_rc y3 0
check "y3: the real corpus selects the four history entries it declares, in corpus order" 'j("cold-ravel-diagnostic.json")["entries"] == ["mb_full_range_avg_over_time","mb_long_range_7d_avg","mb_long_range_subquery_max","mb_long_range_predict_linear"]'
OUT="${TMP}/y3/out"
check "y3: the selection matches a fresh read of the real corpus file" 'j("cold-ravel-diagnostic.json")["entries"] == [e["id"] for e in json.load(open("'"${HERE}"'/../../../benchmarks/metrics/metrics.corpus.json"))["entries"] if (e["class"] == "long_range" or e["id"] == "mb_full_range_avg_over_time") and "history" in e.get("profiles", ["history"])]'

METRICSBENCH_CORPUS="${TMP}/corpus.json" STUB_RESTART_EXIT=1 run_cold y4
expect_rc y4 0
check "y4: every failed restart is a recorded run, stamped, the run passes" 'j("stamps.json")["status"] == "passed" and j("stamps.json")["cold_ravel_diagnostic"]["failed_runs"] == 15 and all(r["status"] == "failed: docker compose restart --no-deps ravel-server failed" for r in j("cold-ravel-diagnostic.json")["runs"])'
expect_no_log "y4: no cold query after a failed restart" "--out ${OUT}/cold-ravel-diagnostic/"

METRICSBENCH_CORPUS="${TMP}/corpus.json" STUB_RESTART_NOT_READY=1 run_cold y5
expect_rc y5 0
check "y5: a failed readiness wait is a recorded run, stamped, the run passes, and the remaining runs are skipped without another wait" 'j("stamps.json")["status"] == "passed" and j("stamps.json")["cold_ravel_diagnostic"]["failed_runs"] == 15 and [r["status"] for r in j("cold-ravel-diagnostic.json")["runs"]].count("failed: ravel did not answer 200 after the restart") == 1 and sum(1 for r in j("cold-ravel-diagnostic.json")["runs"] if r["status"].startswith("failed: skipped after ravel did not answer 200 following ")) == 14 and "readback-folded" in j("stamps.json")["steps_completed"]'
expect_no_log "y5: no cold query after a failed readiness wait" "--out ${OUT}/cold-ravel-diagnostic/"
if [ "$(grep -c ' restart --no-deps ravel-server$' "${LOG}")" -eq 1 ]; then pass "y5: one restart, none for the skipped runs"; else fail "y5: restarts $(grep -c ' restart --no-deps ravel-server$' "${LOG}"), want 1"; fi

METRICSBENCH_CORPUS="${TMP}/corpus.json" STUB_COLD_DROP_RECORD=zz_long_any.3 run_cold y6
expect_rc y6 0
expect_err "y6: the lost record is reported" "could not record zz_long_any repetition 3"
check "y6: a missing record is a failed run naming its entry and repetition, the count still 3 x 5" 'len(j("cold-ravel-diagnostic.json")["runs"]) == 15 and [(r["entry"], r["repetition"]) for r in j("cold-ravel-diagnostic.json")["runs"] if r["ravel_cost"] is None] == [("zz_long_any", 3)] and [r["status"] for r in j("cold-ravel-diagnostic.json")["runs"] if r["ravel_cost"] is None] == ["failed: no record was written for this run"] and j("stamps.json")["cold_ravel_diagnostic"]["failed_runs"] == 1 and j("stamps.json")["cold_ravel_diagnostic"]["missing_runs"] == [{"entry": "zz_long_any", "repetition": 3}] and j("stamps.json")["status"] == "passed"'

cp "${TMP}/corpus.json" "${TMP}/corpus-y7.json"
METRICSBENCH_CORPUS="${TMP}/corpus-y7.json" STUB_CORPUS_CLOBBER=1 run_cold y7
expect_rc y7 0
expect_no_file y7 cold-ravel-diagnostic.json
check "y7: a corpus that cannot be loaded is a stamped failure, not not applicable" 'j("stamps.json")["cold_ravel_diagnostic"] == "failed: the corpus '"${TMP}"'/corpus-y7.json could not be loaded (JSONDecodeError)" and j("stamps.json")["status"] == "passed"'
expect_no_log "y7: nothing restarted for it" " restart --no-deps ravel-server"

# --- z: MB_STAGE1 (ADR-3694 Stage 1, issue #3968) ------------------------------------
# z0: the rendered default command line. ravel.yml's entrypoint with an empty
# RAVEL_SERVER_EXTRA_FLAGS must be exactly the image's ENTRYPOINT, so the
# server's argv is what it was before the variable existed.
RAVEL_YML="${HERE}/../ravel.yml"
DOCKERFILE="${HERE}/../../../Dockerfile"
if grep -qxF '    entrypoint: /usr/local/bin/ravel-server ${RAVEL_SERVER_EXTRA_FLAGS:-}' "${RAVEL_YML}" &&
  [ "$(grep -v '^ *#' "${RAVEL_YML}" | grep -c 'RAVEL_SERVER_EXTRA_FLAGS')" -eq 1 ] &&
  grep -qxF 'ENTRYPOINT ["/usr/local/bin/ravel-server"]' "${DOCKERFILE}"; then
  pass "z0: ravel.yml's entrypoint is the image's ENTRYPOINT followed only by RAVEL_SERVER_EXTRA_FLAGS, empty by default"
else
  fail "z0: ravel.yml's entrypoint line or the Dockerfile's ENTRYPOINT is not the expected text"
fi
# Rendered by compose itself where the host has it (not under the stubs).
if docker compose version >/dev/null 2>&1; then
  for flags in "" "--fold-key-index-leaves --fold-metrics-label-index"; do
    if RAVEL_SERVER_EXTRA_FLAGS="${flags}" RAVEL_IMAGE=stub S3S_FS_BIN=/stub \
      docker compose -f "${HERE}/../docker-compose.yml" -f "${RAVEL_YML}" config --format json >"${TMP}/z0-config.json" 2>"${TMP}/z0-config.err" &&
      python3 -I - "${TMP}/z0-config.json" "${flags}" <<'PY'; then
import json, sys
s = json.load(open(sys.argv[1]))["services"]["ravel-server"]
want = ["/usr/local/bin/ravel-server"] + sys.argv[2].split()
sys.exit(0 if s["entrypoint"] == want and s["command"][:2] == ["--mode", "all"] else 1)
PY
      pass "z0: compose renders the entrypoint as the image's plus '${flags}'"
    else
      fail "z0: compose's rendered entrypoint with RAVEL_SERVER_EXTRA_FLAGS='${flags}' is not the image's plus those flags ($(cat "${TMP}/z0-config.err"))"
    fi
  done
fi

CORPUS_IDS="$(python3 -I -c 'import json,sys; print(json.dumps([e["id"] for e in json.load(open(sys.argv[1]))["entries"]]))' "${HERE}/../../../benchmarks/metrics/metrics.corpus.json")"
UNROUTED_STATE="folded: watermark_hour=490001; parts=3; label_value leaf parts=0; ravel-server --disable-fold --fold-key-index-leaves"
ROUTED_STATE="folded: watermark_hour=490001; parts=3; label_value leaf parts=3; ravel-server --disable-fold --fold-key-index-leaves --fold-metrics-label-index"
INSPECT_SOURCE="ravel-cli catalog inspect --tenant metricsbench --signal metrics: the snapshot HEAD's watermark_hour, parts and per-part key_index (field 8) leaf refs, all fields of the HEAD object itself (the command also GETs every part object, whose figures are not used here)"
run_stage1() {
  local name="$1" prof="$2"
  shift 2
  OUT="${TMP}/${name}/out"
  run_case "$name" --profile "$prof" --ravel-image "${IMAGE}" --ravel-source-commit "${COMMIT}" \
    --bin-dir "${BIN}" --out-dir "${OUT}" --anchor-ms "${ANCHOR}" "$@"
}

MB_STAGE1=1 run_stage1 z cardinality
expect_rc z 0
check "z: the Stage 1 steps follow readback-folded in order" 'j("stamps.json")["steps_completed"][j("stamps.json")["steps_completed"].index("readback-folded"):] == ["readback-folded","stage1-fold","stage1-unrouted","stage1-leaves","stage1-restart","stage1-routed","stage1-holders"] and j("stamps.json")["status"] == "passed"'
if grep -qx 'docker-env up RAVEL_SERVER_EXTRA_FLAGS=--fold-key-index-leaves' "${LOG}"; then
  pass "z: compose up runs ravel-server with --fold-key-index-leaves only"
else
  fail "z: compose up's RAVEL_SERVER_EXTRA_FLAGS is not --fold-key-index-leaves: $(grep 'RAVEL_SERVER_EXTRA_FLAGS' "${LOG}")"
fi
if [ "$(grep '^docker-env recreate ' "${LOG}")" = "docker-env recreate RAVEL_SERVER_EXTRA_FLAGS=--disable-fold --fold-key-index-leaves
docker-env recreate RAVEL_SERVER_EXTRA_FLAGS=--disable-fold --fold-key-index-leaves
docker-env recreate RAVEL_SERVER_EXTRA_FLAGS=--disable-fold --fold-key-index-leaves --fold-metrics-label-index" ] &&
  [ "$(grep -cE '^docker compose .* up -d --no-deps --force-recreate ravel-server$' "${LOG}")" -eq 3 ]; then
  pass "z: three recreates of ravel-server alone, every one with --disable-fold: stage1-fold's and stage1-unrouted's unrouted, stage1-restart's with the label index"
else
  fail "z: the recreates are not the three listed: $(grep '^docker-env recreate ' "${LOG}")"
fi
if python3 -I - "${LOG}" <<'PY'; then
import re, sys
lines = open(sys.argv[1]).read().splitlines()
folds = [re.search(r" catalog fold --tenant \S+ --shards \S+ (.*)$", l).group(1)
         for l in lines if l.startswith("docker compose ") and " catalog fold " in l]
sys.exit(0 if folds == ["--signal metrics --writers-stopped",
                        "--signal metrics --json",
                        "--rebuild-key-index --signal metrics --metrics-label-index --json"] else 1)
PY
  pass "z: the three folds pass exactly their listed flags: the folded step's, stage1-fold's, stage1-leaves'"
else
  fail "z: the catalog fold calls do not carry exactly the listed flags"
fi
if python3 -I - "${LOG}" "${OUT}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
out = sys.argv[2]
def first(pred):
    return next(i for i, l in enumerate(lines) if pred(l))
def every(pred):
    return [i for i, l in enumerate(lines) if pred(l)]
readback = first(lambda l: l.startswith("metricsbench_ingest --readback-only"))
fold = first(lambda l: " catalog fold " in l and l.endswith("--signal metrics --json"))
unrouted = every(lambda l: l.startswith("metricsbench_query ") and "--pass-label folded-unrouted" in l)
leaves = first(lambda l: " --rebuild-key-index " in l)
recreate = every(lambda l: l.startswith("docker-env recreate "))
ready = every(lambda l: l.startswith("curl ") and l.endswith(":4318/api/v1/status/buildinfo"))
# The object spans capture also lists and inspects, before the first recreate.
start = recreate[0] if recreate else len(lines)
inspect = [i for i in every(lambda l: l.startswith("docker compose ") and " catalog inspect " in l) if i > start]
routed = every(lambda l: l.startswith("metricsbench_query ") and "--pass-label folded-routed" in l)
listing = [i for i in every(lambda l: l.startswith("docker compose ") and " catalog list " in l) if i > start]
listing = listing[0] if len(listing) == 1 else -1
sync = first(lambda l: " s3 sync " in l)
holders = first(lambda l: l.startswith("metricsbench_holders "))
down = first(lambda l: l.endswith(" down -v"))
def ready_between(a, b):
    return any(a < r < b for r in ready)
ok = (len(recreate) == 3 and len(inspect) == 2 and len(unrouted) == 6 and len(routed) == 6
      and readback < recreate[0] and ready_between(recreate[0], fold)
      and fold < recreate[1] and ready_between(recreate[1], inspect[0]) and inspect[0] < unrouted[0]
      and unrouted[-1] < leaves < recreate[2] and ready_between(recreate[2], inspect[1])
      and inspect[1] < routed[0] and routed[-1] < listing < sync < holders < down)
if not ok:
    print(dict(readback=readback, recreate=recreate, ready=ready, fold=fold, inspect=inspect,
               unrouted=unrouted, leaves=leaves, routed=routed, listing=listing, sync=sync,
               holders=holders, down=down))
sys.exit(0 if ok else 1)
PY
  pass "z: readback-folded, recreate, stage1-fold, recreate, inspect, unrouted (1 steady + 5 cold), leaves, recreate, inspect, routed (1 + 5), list, holders, teardown, each recreate followed by readiness"
else
  fail "z: the Stage 1 calls are not in their order"
fi
if [ "$(grep '^metricsbench_query ' "${LOG}" | grep -- '--pass-label folded-' | grep -cE -- 'name=(victoriametrics|mimir)')" -eq 0 ] &&
  [ "$(grep '^metricsbench_query ' "${LOG}" | grep -- "--out ${OUT}/stage1-unrouted/report.json" | grep -c -- '--engine name=prometheus,url=http://127.0.0.1:9090 --engine name=ravel,url=http://127.0.0.1:4318,bearer_env=METRICSBENCH_RAVEL_TOKEN')" -eq 1 ]; then
  pass "z: the Stage 1 passes query Prometheus and Ravel only"
else
  fail "z: a Stage 1 pass queries an engine other than Prometheus and Ravel"
fi
expect_log "z: the unrouted pass names its fold state" "--pass-label folded-unrouted --fold-state ${UNROUTED_STATE}"
expect_log "z: the routed pass names its fold state" "--pass-label folded-routed --fold-state ${ROUTED_STATE}"
expect_log "z: the Stage 1 provenance is marked a non-comparable regime" "--config diagnostic_regime=stage1-folded-routed --config comparable=false"
for f in stage1-fold.json stage1-leaves.json stage1-unrouted/report.json stage1-unrouted/diagnostics.json stage1-unrouted/cold.json \
  stage1-routed/report.json stage1-routed/diagnostics.json stage1-routed/cold.json stage1-compare.json holders.json; do
  expect_file z "$f"
done
check "z: stage1 stamped on with every flag set" 'j("stamps.json")["stage1"]["enabled"] is True and j("stamps.json")["stage1"]["server_flags_up"] == "--fold-key-index-leaves" and j("stamps.json")["stage1"]["server_flags_unrouted"] == "--disable-fold --fold-key-index-leaves" and j("stamps.json")["stage1"]["server_flags_routed"] == "--disable-fold --fold-key-index-leaves --fold-metrics-label-index"'
check "z: the background fold is stamped off from stage1-fold onward" 'j("stamps.json")["stage1"]["background_fold"].startswith("off from stage1-fold onward: ravel-server was recreated with --disable-fold before stage1-fold")'
check "z: stage1-fold's and stage1-unrouted's recreates are stamped with their flags" 'all(j("stamps.json")["stage1"][k] == "recreated: docker compose up -d --no-deps --force-recreate ravel-server with RAVEL_SERVER_EXTRA_FLAGS='"'"'--disable-fold --fold-key-index-leaves'"'"', then ravel answered 200 on /api/v1/status/buildinfo" for k in ("fold_restart", "unrouted_restart"))'
check "z: stage1.unrouted_catalog stamps the HEAD watermark, the part count and no label_value leaf part, naming its source" 'j("stamps.json")["stage1"]["unrouted_catalog"] == {"source": "'"${INSPECT_SOURCE}"'", "file": "stage1-unrouted.inspect.txt", "watermark_hour": 490001, "parts": 3, "label_value_leaf_parts": 0, "label_value_leaf_refs": 0}'
check "z: stage1.routed_catalog stamps every part carrying a label_value leaf" 'j("stamps.json")["stage1"]["routed_catalog"] == {"source": "'"${INSPECT_SOURCE}"'", "file": "stage1-routed.inspect.txt", "watermark_hour": 490001, "parts": 3, "label_value_leaf_parts": 3, "label_value_leaf_refs": 3}'
expect_file z stage1-unrouted.inspect.txt
expect_file z stage1-routed.inspect.txt
check "z: stage1.fold is the fold report with its wall time" 'j("stamps.json")["stage1"]["fold"]["file"] == "stage1-fold.json" and isinstance(j("stamps.json")["stage1"]["fold"]["wall_ms"], int) and {k: j("stamps.json")["stage1"]["fold"][k] for k in ("watermark_hour","no_op","get_requests","key_index_leaves_written")} == {"watermark_hour": 490001, "no_op": True, "get_requests": 6, "key_index_leaves_written": 0}'
check "z: stage1.leaves records the rebuild's leaf, part and byte figures and wall time" '{k: j("stamps.json")["stage1"]["leaves"][k] for k in ("leaves_written","leaf_bytes_written","parts_rebuilt","parts_over_cap","uncovered_entries","attempt_count","file")} == {"leaves_written": 12, "leaf_bytes_written": 4096, "parts_rebuilt": 3, "parts_over_cap": 0, "uncovered_entries": 0, "attempt_count": 2, "file": "stage1-leaves.json"} and isinstance(j("stamps.json")["stage1"]["leaves"]["wall_ms"], int)'
check "z: stage1.leaves records every rebuild request under its own name, the per-batch ones summed over attempts" 'j("stamps.json")["stage1"]["leaves"]["requests"] == {"data_object_ranged_gets": 7, "leaf_prefix_gets": 2, "erasure_list_requests": 1, "erasure_get_requests": 1, "part_object_gets": 5, "leaf_puts": 20, "head_cas_puts": 3, "not_reported": "GETs of the snapshot HEAD (the rebuild report does not count them)"}'
check "z: stage1.unrouted and stage1.routed name their label, state and files" 'j("stamps.json")["stage1"]["unrouted"] == {"pass_label": "folded-unrouted", "fold_state": "'"${UNROUTED_STATE}"'", "diagnostics": "stage1-unrouted/diagnostics.json", "report": "stage1-unrouted/report.json"} and j("stamps.json")["stage1"]["routed"] == {"pass_label": "folded-routed", "fold_state": "'"${ROUTED_STATE}"'", "diagnostics": "stage1-routed/diagnostics.json", "report": "stage1-routed/report.json"}'
check "z: stage1.restart says what was recreated" 'j("stamps.json")["stage1"]["restart"].startswith("recreated: docker compose up -d --no-deps --force-recreate ravel-server with RAVEL_SERVER_EXTRA_FLAGS='"'"'--disable-fold --fold-key-index-leaves --fold-metrics-label-index'"'"'")'
check "z: every steady record carries its pass label and fold state" 'all(r["pass_label"] == "folded-unrouted" and r["fold_state"] == "'"${UNROUTED_STATE}"'" for r in j("stage1-unrouted/diagnostics.json")["ravel_cost"]) and all(r["pass_label"] == "folded-routed" and r["fold_state"] == "'"${ROUTED_STATE}"'" for r in j("stage1-routed/diagnostics.json")["ravel_cost"])'
check "z: the cold selection runs in both passes, labelled, in its own regime" 'all(j(f"stage1-{w}/cold.json")["regime"] == f"stage1-folded-{w}-cold" and j(f"stage1-{w}/cold.json")["entries"] == ["mb_full_range_avg_over_time"] and len(j(f"stage1-{w}/cold.json")["runs"]) == 5 and all(r["ravel_cost"]["pass_label"] == f"folded-{w}" for r in j(f"stage1-{w}/cold.json")["runs"]) for w in ("unrouted", "routed")) and j("stamps.json")["stage1"]["routed_cold"]["file"] == "cold.json" and j("stamps.json")["stage1"]["routed_cold"]["failed_runs"] == 0'
check "z: stage1-compare.json compares every entry's hashes and statuses and counts each pass's records" 'j("stage1-compare.json")["schema"] == "metricsbench-stage1-compare/5" and j("stage1-compare.json")["entries_compared"] == len('"${CORPUS_IDS}"') == j("stage1-compare.json")["entries_equal"] and j("stage1-compare.json")["entries_multi_hash_count"] == 0 and j("stage1-compare.json")["entries_differing_count"] == 0 and j("stage1-compare.json")["entries_without_hash_count"] == 0 and j("stage1-compare.json")["statements_status_differing_count"] == 0 and j("stage1-compare.json")["statements_status_differing"] == [] and j("stage1-compare.json")["status_check"].startswith("per entry, the set of statuses its stage1-unrouted records carry == the set its stage1-routed records carry (ADR-3694 row 9)") and j("stage1-compare.json")["passes"] == {"unrouted": {"records": len('"${CORPUS_IDS}"'), "records_answered": len('"${CORPUS_IDS}"'), "records_answered_null_hash": 0, "records_status_unexpected": 0, "records_label_index_null": 0, "records_pruned_by_label_index": 0, "segments_pruned_by_label_index": 0}, "routed": {"records": len('"${CORPUS_IDS}"'), "records_answered": len('"${CORPUS_IDS}"'), "records_answered_null_hash": 0, "records_status_unexpected": 0, "records_label_index_null": 0, "records_pruned_by_label_index": len('"${CORPUS_IDS}"'), "segments_pruned_by_label_index": 3 * len('"${CORPUS_IDS}"')}} and j("stamps.json")["stage1"]["compare"]["entries_differing"] == [] and j("stamps.json")["stage1"]["compare"]["statements_status_differing"] == []'
expect_log "z: the holders listing is catalog list --json" "catalog list --tenant metricsbench --shards 4 --hours"
expect_log "z: the holders download takes the L0 and L1 data objects of Ravel's bucket through the s3s endpoint" "--entrypoint aws createbuckets --endpoint-url http://s3s:9000 s3 sync s3://ravel-metricsbench/t/ /stage1-objects/t/ --exclude * --include */m/l0/*.rseg --include */m/l1/*.rseg --only-show-errors"
expect_log "z: the download lands in the run's out dir" "-v ${OUT}/stage1-objects:/stage1-objects"
expect_log "z: every object the listing names was downloaded for metricsbench_holders" "metricsbench_holders saw 300 of 300 listed objects"
expect_log "z: metricsbench_holders gets the corpus, ingest report, workload, listing and objects" "/benchmarks/metrics/metrics.corpus.json --ingest-report ${OUT}/ingest.json --workload "
expect_log "z: metricsbench_holders reads the listing beside the objects" "--objects-list ${OUT}/stage1-objects.jsonl --objects-dir ${OUT}/stage1-objects --out ${OUT}/holders.json"
expect_no_file z stage1-objects
expect_file z stage1-objects.jsonl
check "z: stage1.holders stamps the method, the counts and the window, holder and value-holder figures per statement, every window checked against segmentsFetched and every routed prune against expected_routed_prunes" '{k: v for k, v in j("stamps.json")["stage1"]["holders"].items() if not k.endswith("_check")} == {"file": "holders.json", "bucket": "ravel-metricsbench", "method": "stub method", "listing_state": "routed: listed after stage1-routed, objects_above_watermark against stage1.routed_catalog.watermark_hour", "listing_watermark_hour": 490001, "objects_listed": 300, "objects_above_watermark": 0, "objects_l0": 300, "objects_l1": 0, "series_total": 40, "min_commit_tokens": 1, "token_objects": 1, "tokens_unmatched": [], "statements": [{"id": i, "segments_in_window": 10, "named_in_window": 4, "name_pruned": True, "token_only_fetched": 0, "unrouted_fetched_expected": 4, "holders": 2, "value_holders": 1, "expected_routed_prunes": 3, "route_bound": True, "route_bound_reason": None, "routed_pruned_by_label_index": [3]} for i in '"${CORPUS_IDS}"'], "skipped": 1, "window_mismatches_count": 0, "window_mismatches": [], "window_unchecked_count": 0, "window_unchecked": [], "routed_prune_mismatches_count": 0, "routed_prune_mismatches": [], "routed_prune_unchecked_count": 0, "routed_prune_unchecked": [], "routed_leaf_unread_count": 0, "routed_leaf_unread": [], "route_unbound_count": 0, "route_unbound": []}'
check "z: each check sentence names what it compares" 'sorted(k for k in j("stamps.json")["stage1"]["holders"] if k.endswith("_check")) == ["bound_check", "route_bound_check", "routed_leaf_check", "routed_prune_check", "window_check"] and all(w in j("stamps.json")["stage1"]["holders"]["window_check"] for w in ("segmentsFetched of every stage1-unrouted record", "== unrouted_fetched_expected", "token_only_fetched", "min_commit_token", "unchecked")) and all(w in j("stamps.json")["stage1"]["holders"]["routed_prune_check"] for w in ("segmentsPrunedByLabelIndex of every stage1-routed record", "== expected_routed_prunes", "unchecked")) and all(w in j("stamps.json")["stage1"]["holders"]["routed_leaf_check"] for w in ("keyIndex phase", "s3GetRequests", "cacheHits")) and all(w in j("stamps.json")["stage1"]["holders"]["bound_check"] for w in ("series-level index", "not the label-value router", "value_holders", "LABEL_DICT", "the router'"'"'s bound"))'
check "z: the cold records are counted against the selected entries x repetitions in both passes" 'all(j("stamps.json")["stage1"][f"{w}_cold_records"] == {"applicable": True, "expected_records": 5, "records": 5, "records_short": 0, "basis": f"stage1.{w}_cold: records with a ravel_cost (runs - failed_runs) against entries x repetitions_per_entry"} for w in ("unrouted", "routed"))'
check "z: the query step's records keep their own label" 'all(r["pass_label"] == "background-folded" for r in j("diagnostics.json")["ravel_cost"])'
expect_log "z: the main report's provenance is marked non-comparable with its reason" "--config comparable=false --config comparability_reason=MB_STAGE1=1: ravel-server ran with --fold-key-index-leaves (not the default command line) through this report's passes"
if [ "$(grep '^metricsbench_report provenance ' "${LOG}" | grep -vc -- '--config diagnostic_regime=')" -eq 1 ] &&
  [ "$(grep '^metricsbench_report provenance ' "${LOG}" | grep -cE -- '--config diagnostic_regime=stage1-folded-(unrouted|routed) ')" -eq 2 ] &&
  [ "$(grep '^metricsbench_report provenance ' "${LOG}" | grep -vc -- '--config comparable=false')" -eq 0 ]; then
  pass "z: every provenance of the run carries comparable=false: the main one, both Stage 1 passes' and the cold passes'"
else
  fail "z: a provenance call lacks comparable=false: $(grep '^metricsbench_report provenance ' "${LOG}")"
fi

# z1: on ci the cold selection is empty, stamped not applicable in both passes.
MB_STAGE1=1 run_stage1 z1 ci
expect_rc z1 0
check "z1: ci runs every Stage 1 step" 'j("stamps.json")["steps_completed"][-6:] == ["stage1-fold","stage1-unrouted","stage1-leaves","stage1-restart","stage1-routed","stage1-holders"]'
check "z1: each routed record's per-value route figures: one __name__ pass, its cache hits, 3 parts covered, the (1 + n) x covering bound, nothing over it or beyond the ceiling" 'j("stage1-compare.json")["routed_values"] == [{"entry": i, "rep": 1, "n": 1, "leaf_gets": 0, "leaf_cache_hits": 2, "covering_parts": 3, "leaf_gets_bound": 6, "parts_beyond_ceiling": 0, "name_pruned_alone": True} for i in '"${CORPUS_IDS}"'] and {k: j("stage1-compare.json")[k] for k in ("routed_records_with_values", "routed_records_without_values", "routed_records_values_unreadable_count", "statements_leaf_gets_over_bound_count", "statements_leaf_gets_over_bound", "parts_beyond_ceiling_total")} == {"routed_records_with_values": len('"${CORPUS_IDS}"'), "routed_records_without_values": 0, "routed_records_values_unreadable_count": 0, "statements_leaf_gets_over_bound_count": 0, "statements_leaf_gets_over_bound": [], "parts_beyond_ceiling_total": 0} and "(1 + n) x covering_parts" in j("stage1-compare.json")["route_values_check"] and "routed_values" not in j("stamps.json")["stage1"]["compare"] and j("stamps.json")["stage1"]["compare"]["routed_records_with_values"] == len('"${CORPUS_IDS}"')'
check "z1: ci's cold passes are not applicable" 'j("stamps.json")["stage1"]["unrouted_cold"].startswith("not applicable: ") and j("stamps.json")["stage1"]["routed_cold"].startswith("not applicable: ")'
check "z1: ci's cold record counts are 0 of 0, naming the not-applicable stamp" 'all({k: v for k, v in j("stamps.json")["stage1"][f"{w}_cold_records"].items() if k != "basis"} == {"applicable": False, "expected_records": 0, "records": 0, "records_short": 0} and j("stamps.json")["stage1"][f"{w}_cold_records"]["basis"].startswith(f"stage1.{w}_cold is '"'"'not applicable: ") for w in ("unrouted", "routed"))'

# z2: a routed hash that differs is recorded, not fatal.
MB_STAGE1=1 STUB_ROUTED_HASH_DIFF=mb_selective_job_rate_sum run_stage1 z2 ci
expect_rc z2 0
check "z2: the differing entry is named with both hashes, the run passes" 'j("stage1-compare.json")["entries_differing"] == [{"entry": "mb_selective_job_rate_sum", "unrouted": ["blake3:mb_selective_job_rate_sum"], "routed": ["blake3:mb_selective_job_rate_sum-routed"]}] and j("stamps.json")["stage1"]["compare"]["entries_differing"] == ["mb_selective_job_rate_sum"] and j("stamps.json")["status"] == "passed"'

# z2b: an entry whose two passes carry the same two hashes is multi-hash, not differing.
MB_STAGE1=1 STUB_TWO_HASH_ENTRY=mb_selective_job_rate_sum run_stage1 z2b ci
expect_rc z2b 0
check "z2b: the two-hash entry is listed as multi-hash with its hashes, not as differing or equal" 'j("stage1-compare.json")["entries_multi_hash_count"] == 1 and j("stage1-compare.json")["entries_multi_hash"] == [{"entry": "mb_selective_job_rate_sum", "hashes": ["blake3:mb_selective_job_rate_sum", "blake3:mb_selective_job_rate_sum-rep2"]}] and j("stamps.json")["stage1"]["compare"]["entries_multi_hash"] == ["mb_selective_job_rate_sum"] and j("stage1-compare.json")["entries_differing_count"] == 0 and j("stage1-compare.json")["entries_differing"] == [] and j("stage1-compare.json")["entries_equal"] == len('"${CORPUS_IDS}"') - 1 and j("stage1-compare.json")["entries_compared"] == len('"${CORPUS_IDS}"')'

# z3: a profile other than cardinality and ci is refused before anything starts.
MB_STAGE1=1 run_stage1 z3 history
expect_rc z3 65
expect_err "z3: the refusal names the profile and the two it allows" "MB_STAGE1=1 with --profile history: the Stage 1 lanes run on cardinality (the pre-registered run) and ci (its rehearsal) only"
expect_no_log "z3: nothing brought up" "ravel.yml up -d"

MB_STAGE1=yes run_stage1 z4 ci
expect_rc z4 64
expect_err "z4: a value other than 1 or 0 is a usage error" "MB_STAGE1 takes 1 (on) or 0 (off), got 'yes'"
run_case z5-off --profile history --bin-dir "${BIN}" --out-dir "${TMP}/z5-off/out" --anchor-ms "${ANCHOR}" --diagnostic-prometheus-max-samples 1000
rc_unset=$rc
MB_STAGE1=1 run_case z5 --profile history --bin-dir "${BIN}" --out-dir "${TMP}/z5/out" --anchor-ms "${ANCHOR}" --diagnostic-prometheus-max-samples 1000
expect_rc z5 64
if [ "${rc_unset}" -eq 0 ]; then pass "z5: the same diagnostic run without MB_STAGE1 exits 0"; else fail "z5: the diagnostic run without MB_STAGE1 exited ${rc_unset}"; fi
expect_err "z5: the diagnostic mode refuses MB_STAGE1" "MB_STAGE1=1 measures the folded Ravel passes"

MB_STAGE1=0 run_stage1 z6 ci
expect_rc z6 0
check "z6: MB_STAGE1=0 is off" 'j("stamps.json")["stage1"] == {"enabled": False} and j("stamps.json")["steps_completed"][-1] == "readback-folded"'

# z7: a failed leaf rebuild ends the run before the restart.
MB_STAGE1=1 STUB_STAGE1_LEAVES_EXIT=1 run_stage1 z7 ci
expect_rc z7 70
check "z7: stage1-leaves named, its stamp says which fold failed" 'j("stamps.json")["failed_step"] == "stage1-leaves" and j("stamps.json")["stage1"]["leaves"] == "failed: ravel-cli catalog fold --rebuild-key-index --signal metrics --metrics-label-index --json exited 1" and j("stamps.json")["stage1"]["restart"] is None'
expect_no_log "z7: no recreate with the label index" "docker-env recreate RAVEL_SERVER_EXTRA_FLAGS=--disable-fold --fold-key-index-leaves --fold-metrics-label-index"
expect_no_log "z7: no routed pass" "--pass-label folded-routed"
expect_log "z7: torn down" " down -v"

# z8: ravel-server not ready after each of the three recreates in turn.
MB_STAGE1=1 STUB_RECREATE_NOT_READY=1 run_stage1 z8a ci
expect_rc z8a 70
check "z8a: stage1-fold named, nothing after it ran" 'j("stamps.json")["failed_step"] == "stage1-fold" and "stage1-fold" not in j("stamps.json")["steps_completed"]'
expect_no_log "z8a: no Stage 1 fold" "--signal metrics --json"
MB_STAGE1=1 STUB_RECREATE_NOT_READY=2 run_stage1 z8b ci
expect_rc z8b 70
check "z8b: stage1-unrouted named" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["steps_completed"][-1] == "stage1-fold"'
expect_no_log "z8b: no unrouted pass" "--pass-label folded-unrouted"
MB_STAGE1=1 STUB_RECREATE_NOT_READY=3 run_stage1 z8 ci
expect_rc z8 70
check "z8: stage1-restart named" 'j("stamps.json")["failed_step"] == "stage1-restart" and "stage1-routed" not in j("stamps.json")["steps_completed"]'
expect_no_log "z8: no routed pass" "--pass-label folded-routed"

# z9: after a failed ingest verification the Stage 1 steps are skipped and say why.
MB_STAGE1=1 STUB_INGEST_READBACK_FAIL=mimir run_stage1 z9 ci
expect_rc z9 70
check "z9: every Stage 1 stamp says skipped and why" 'all(j("stamps.json")["stage1"][k].startswith("skipped: the ingest verification failed") for k in ("fold","unrouted","unrouted_cold","unrouted_cold_records","leaves","restart","routed","routed_cold","routed_cold_records","compare","holders"))'
expect_no_log "z9: no Stage 1 fold" "--signal metrics --json"

# z10: a failed download is stage1-holders' failure, and leaves nothing behind.
MB_STAGE1=1 STUB_SYNC_EXIT=1 run_stage1 z10 ci
expect_rc z10 70
check "z10: stage1-holders named, the sync named in its stamp" 'j("stamps.json")["failed_step"] == "stage1-holders" and j("stamps.json")["stage1"]["holders"] == "failed: aws s3 sync of s3://ravel-metricsbench/t/ exited 1"'
expect_no_file z10 stage1-objects
expect_no_log "z10: no count without the objects" "metricsbench_holders --corpus"

# z11: on the real-S3 lane the download reads PREFIX-ravel with no endpoint override.
MB_STAGE1=1 run_stage1 z11 ci --real-s3-bucket-prefix mb-run1 --real-s3-region us-east-1
expect_rc z11 0
expect_log "z11: the real-S3 download reads PREFIX-ravel" "--entrypoint aws createbuckets s3 sync s3://mb-run1-ravel/t/ /stage1-objects/t/"
check "z11: the bucket is stamped" 'j("stamps.json")["stage1"]["holders"]["bucket"] == "mb-run1-ravel"'

# z12: a label_value leaf already in the snapshot refuses the unrouted pass.
MB_STAGE1=1 STUB_INSPECT_LEAF_PARTS=1 run_stage1 z12 ci
expect_rc z12 70
check "z12: stage1-unrouted named, the refusal stamped with the inspected state" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted"] == "refused: 1 snapshot parts carry a label_value leaf before the unrouted pass" and j("stamps.json")["stage1"]["unrouted_catalog"]["label_value_leaf_parts"] == 1'
expect_no_log "z12: no unrouted pass" "--pass-label folded-unrouted"

# z13: fewer label_value leaf parts than stage1-leaves rebuilt refuses the routed pass.
MB_STAGE1=1 STUB_INSPECT_REBUILT_LEAF_PARTS=2 run_stage1 z13 ci
expect_rc z13 70
check "z13: stage1-routed named, the refusal names both counts" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: 2 snapshot parts carry a label_value leaf, stage1-leaves rebuilt 3"'
expect_no_log "z13: no routed pass" "--pass-label folded-routed"

# z14: an inspect whose part blocks disagree with its parts line is unreadable.
MB_STAGE1=1 STUB_INSPECT_PARTS_LINE=4 run_stage1 z14 ci
expect_rc z14 70
check "z14: stage1-unrouted named, the stamp says what did not add up" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted_catalog"] == "failed: stage1-unrouted.inspect.txt lists 3 parts, its parts line says 4"'
expect_no_log "z14: no unrouted pass" "--pass-label folded-unrouted"

# z15: a routed record that failed is counted, not fatal; the band check catches it.
MB_STAGE1=1 STUB_ROUTED_FAILED_ENTRY=mb_selective_job_rate_sum run_stage1 z15 ci
expect_rc z15 0
check "z15: the failed record is counted in the routed pass, its status differs from the unrouted pass and the entry has no hash" 'j("stage1-compare.json")["passes"]["routed"]["records_status_unexpected"] == 1 and j("stage1-compare.json")["passes"]["routed"]["records_answered_null_hash"] == 0 and j("stage1-compare.json")["passes"]["routed"]["records_label_index_null"] == 1 and j("stage1-compare.json")["passes"]["unrouted"]["records_status_unexpected"] == 0 and j("stage1-compare.json")["statements_status_differing"] == [{"entry": "mb_selective_job_rate_sum", "unrouted": ["ok"], "routed": ["failed: stub"]}] and "mb_selective_job_rate_sum" in j("stage1-compare.json")["entries_without_hash"]'

# z16: an unrouted record the label index pruned is counted.
MB_STAGE1=1 STUB_UNROUTED_PRUNED_ENTRY=mb_selective_job_rate_sum run_stage1 z16 ci
expect_rc z16 0
check "z16: one unrouted record pruned by the label index" 'j("stage1-compare.json")["passes"]["unrouted"]["records_pruned_by_label_index"] == 1 and j("stage1-compare.json")["passes"]["unrouted"]["segments_pruned_by_label_index"] == 2'

# z17: an unrouted segmentsFetched that is not unrouted_fetched_expected is a mismatch.
MB_STAGE1=1 STUB_HOLDERS_WINDOW_OFF=mb_selective_job_rate_sum run_stage1 z17 ci
expect_rc z17 0
check "z17: the statement named with its expected, named and window counts and the fetched figures" 'j("stamps.json")["stage1"]["holders"]["window_mismatches_count"] == 1 and j("stamps.json")["stage1"]["holders"]["window_mismatches"] == [{"id": "mb_selective_job_rate_sum", "unrouted_fetched_expected": 5, "named_in_window": 5, "segments_in_window": 10, "token_only_fetched": 0, "unrouted_fetched": [4]}] and j("stamps.json")["stage1"]["holders"]["window_unchecked_count"] == 0'

# z18: a statement with no count is unchecked, never a match.
MB_STAGE1=1 STUB_HOLDERS_WINDOW_NULL=mb_selective_job_rate_sum run_stage1 z18 ci
expect_rc z18 0
check "z18: the statement unchecked, none mismatched" 'j("stamps.json")["stage1"]["holders"]["window_unchecked_count"] == 1 and j("stamps.json")["stage1"]["holders"]["window_mismatches_count"] == 0'

# z20: an in-window object outside the name's postings (7 in the window, 4
# named, the unrouted pass fetching 4 and pruning 6) and a statement naming an
# absent metric (7 in the window, 0 named, fetching 0 and pruning 10) are both
# matches: segmentsFetched is compared with unrouted_fetched_expected, not
# fetched + pruned with segments_in_window.
MB_STAGE1=1 STUB_HOLDERS_NAMED_SPLIT=mb_single_gauge_instant STUB_ABSENT_ENTRY=mb_absent_instant run_stage1 z20 ci
expect_rc z20 0
check "z20: both statements carry their split figures, none mismatched or unchecked" '[{k: s[k] for k in ("segments_in_window", "named_in_window", "unrouted_fetched_expected")} for s in j("stamps.json")["stage1"]["holders"]["statements"] if s["id"] in ("mb_absent_instant", "mb_single_gauge_instant")] == [{"segments_in_window": 7, "named_in_window": 0, "unrouted_fetched_expected": 0}, {"segments_in_window": 7, "named_in_window": 4, "unrouted_fetched_expected": 4}] and j("stamps.json")["stage1"]["holders"]["window_mismatches_count"] == 0 and j("stamps.json")["stage1"]["holders"]["window_unchecked_count"] == 0'

# z21: a statement incorrect in both passes (a 1-ULP gap the oracle reads as
# incorrect) is an answered status that routing did not change.
MB_STAGE1=1 STUB_INCORRECT_ENTRY=mb_single_counter_rate run_stage1 z21 ci
expect_rc z21 0
check "z21: incorrect in both passes is no status difference and no unexpected status" 'j("stage1-compare.json")["statements_status_differing_count"] == 0 and all(j("stage1-compare.json")["passes"][w]["records_status_unexpected"] == 0 and j("stage1-compare.json")["passes"][w]["records_answered"] == len('"${CORPUS_IDS}"') for w in ("unrouted", "routed")) and j("stage1-compare.json")["entries_differing_count"] == 0'

# z22: a status the routed pass changes is a row 9 difference, though both are answered.
MB_STAGE1=1 STUB_ROUTED_STATUS_ENTRY=mb_single_counter_rate run_stage1 z22 ci
expect_rc z22 0
check "z22: the entry named with both status sets" 'j("stage1-compare.json")["statements_status_differing"] == [{"entry": "mb_single_counter_rate", "unrouted": ["ok"], "routed": ["incorrect"]}] and j("stamps.json")["stage1"]["compare"]["statements_status_differing"] == ["mb_single_counter_rate"] and j("stage1-compare.json")["passes"]["routed"]["records_status_unexpected"] == 0'

# z23: a statement with a selector lacking a qualifying matcher is listed as not route-bound.
MB_STAGE1=1 STUB_HOLDERS_UNBOUND=mb_selective_error_ratio run_stage1 z23 ci
expect_rc z23 0
check "z23: the statement listed with its reason" 'j("stamps.json")["stage1"]["holders"]["route_unbound_count"] == 1 and j("stamps.json")["stage1"]["holders"]["route_unbound"] == [{"id": "mb_selective_error_ratio", "reason": "stub: no qualifying matcher"}] and [s["route_bound"] for s in j("stamps.json")["stage1"]["holders"]["statements"] if s["id"] == "mb_selective_error_ratio"] == [False]'

# z24: a parts_rebuilt that is not a whole number refuses the routed pass.
MB_STAGE1=1 STUB_STAGE1_PARTS_REBUILT=null run_stage1 z24 ci
expect_rc z24 70
check "z24: stage1-routed named, the refusal names the figure" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: stage1.leaves.parts_rebuilt is '"'"'None'"'"', not a whole number"'
expect_no_log "z24: no routed pass" "--pass-label folded-routed"

# z28: an inspect stamp that was not written, or whose label_value leaf part
# count cannot be read back as a whole number, refuses the pass it gates.
MB_STAGE1=1 STUB_PY_OP=set STUB_PY_KEY=stage1.unrouted_catalog run_stage1 z28a ci
expect_rc z28a 70
check "z28a: stage1-unrouted named, the refusal names the unwritten stamp" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted"] == "refused: stage1.unrouted_catalog could not be stamped (py set exited 1)"'
expect_no_log "z28a: no unrouted pass" "--pass-label folded-unrouted"
MB_STAGE1=1 STUB_PY_OP=set STUB_PY_KEY=stage1.routed_catalog run_stage1 z28b ci
expect_rc z28b 70
check "z28b: stage1-routed named, the refusal names the unwritten stamp" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: stage1.routed_catalog could not be stamped (py set exited 1)"'
expect_no_log "z28b: no routed pass" "--pass-label folded-routed"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.unrouted_catalog.label_value_leaf_parts run_stage1 z28c ci
expect_rc z28c 70
check "z28c: stage1-unrouted named, the refusal names the unread figure" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted"] == "refused: stage1.unrouted_catalog.label_value_leaf_parts could not be read (py get exited 1)"'
expect_no_log "z28c: no unrouted pass" "--pass-label folded-unrouted"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.routed_catalog.label_value_leaf_parts run_stage1 z28d ci
expect_rc z28d 70
check "z28d: stage1-routed named, the refusal names the unread figure" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: stage1.routed_catalog.label_value_leaf_parts could not be read (py get exited 1)"'
expect_no_log "z28d: no routed pass" "--pass-label folded-routed"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.unrouted_catalog.label_value_leaf_parts STUB_PY_PRINT=None run_stage1 z28e ci
expect_rc z28e 70
check "z28e: stage1-unrouted named, the refusal names the figure that is not a whole number" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted"] == "refused: stage1.unrouted_catalog.label_value_leaf_parts is '"'"'None'"'"', not a whole number"'
expect_no_log "z28e: no unrouted pass" "--pass-label folded-unrouted"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.routed_catalog.label_value_leaf_parts STUB_PY_PRINT=None run_stage1 z28f ci
expect_rc z28f 70
check "z28f: stage1-routed named, the refusal names the figure that is not a whole number" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: stage1.routed_catalog.label_value_leaf_parts is '"'"'None'"'"', not a whole number"'
expect_no_log "z28f: no routed pass" "--pass-label folded-routed"

# z29: a watermark or parts figure the fold state names that cannot be read
# back as a whole number refuses the pass, as the leaf part count does.
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.unrouted_catalog.watermark_hour run_stage1 z29a ci
expect_rc z29a 70
check "z29a: stage1-unrouted named, the refusal names the unread watermark" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted"] == "refused: stage1.unrouted_catalog.watermark_hour could not be read (py get exited 1)"'
expect_no_log "z29a: no unrouted pass" "--pass-label folded-unrouted"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.routed_catalog.parts run_stage1 z29b ci
expect_rc z29b 70
check "z29b: stage1-routed named, the refusal names the unread parts count" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: stage1.routed_catalog.parts could not be read (py get exited 1)"'
expect_no_log "z29b: no routed pass" "--pass-label folded-routed"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.routed_catalog.watermark_hour STUB_PY_PRINT=None run_stage1 z29c ci
expect_rc z29c 70
check "z29c: stage1-routed named, the watermark is not a whole number" 'j("stamps.json")["failed_step"] == "stage1-routed" and j("stamps.json")["stage1"]["routed"] == "refused: stage1.routed_catalog.watermark_hour is '"'"'None'"'"', not a whole number"'
expect_no_log "z29c: no routed pass" "--pass-label folded-routed"
MB_STAGE1=1 STUB_PY_OP=get STUB_PY_KEY=stage1.unrouted_catalog.parts STUB_PY_PRINT=None run_stage1 z29d ci
expect_rc z29d 70
check "z29d: stage1-unrouted named, the parts count is not a whole number" 'j("stamps.json")["failed_step"] == "stage1-unrouted" and j("stamps.json")["stage1"]["unrouted"] == "refused: stage1.unrouted_catalog.parts is '"'"'None'"'"', not a whole number"'
expect_no_log "z29d: no unrouted pass" "--pass-label folded-unrouted"

# z25: a routed segmentsPrunedByLabelIndex that is not expected_routed_prunes is a mismatch.
MB_STAGE1=1 STUB_HOLDERS_PRUNES_OFF=mb_selective_job_rate_sum run_stage1 z25 ci
expect_rc z25 0
check "z25: the statement named with its expected prunes, value holders and the routed figures" 'j("stamps.json")["stage1"]["holders"]["routed_prune_mismatches_count"] == 1 and j("stamps.json")["stage1"]["holders"]["routed_prune_mismatches"] == [{"id": "mb_selective_job_rate_sum", "expected_routed_prunes": 2, "value_holders": 2, "routed_pruned_by_label_index": [3]}] and j("stamps.json")["stage1"]["holders"]["routed_prune_unchecked_count"] == 0'

# z26: a routed record of a route_bound statement whose keyIndex phase read no
# leaf is counted; a statement that is not route_bound is not.
MB_STAGE1=1 STUB_ROUTED_LEAF_UNREAD_ENTRY=mb_single_counter_rate run_stage1 z26 ci
expect_rc z26 0
check "z26: the statement named with its unread record count" 'j("stamps.json")["stage1"]["holders"]["routed_leaf_unread_count"] == 1 and j("stamps.json")["stage1"]["holders"]["routed_leaf_unread"] == [{"id": "mb_single_counter_rate", "records": 1}]'
MB_STAGE1=1 STUB_ROUTED_LEAF_UNREAD_ENTRY=mb_selective_error_ratio STUB_HOLDERS_UNBOUND=mb_selective_error_ratio run_stage1 z26u ci
expect_rc z26u 0
check "z26u: no unread record counted for a statement that is not route_bound" 'j("stamps.json")["stage1"]["holders"]["routed_leaf_unread_count"] == 0 and j("stamps.json")["stage1"]["holders"]["routed_prune_unchecked_count"] == 0'

# z27: the figures of metricsbench-nightly run 42495422937 (ci, stage1=true),
# copied from its out-dir as issue #3968's fix round 3 quotes them: 8 L0
# objects; mb_absent_selective_labels names a metric no object holds (0 named,
# 0 holders), and both passes' records fetched 4 and pruned 8, the 4 L0 objects
# its 4 shards' min_commit_tokens name, which the resolve adds whatever their
# metric; every routed record says segmentsPrunedByLabelIndex 0, since every
# ci object's LABEL_DICT holds every probed value (value holders = named);
# mb_single_counter_rate's routed keyIndex phase shows 10 cache hits and
# mb_selective_job_rate_sum's 4, both with 0 GETs; three statements name 8
# objects with 2 holders. The run quoted no keyIndex figure for the other
# routed records; they are given mb_selective_job_rate_sum's. The absent
# statement's routed record reads no leaf: its snapshot holds only the token
# objects, which carry no part, so the engine does not route it.
cat >"${TMP}/run-42495422937.json" <<'JSON'
{"holders": {
   "report": {"objects_l0": 8, "objects_l1": 0, "min_commit_tokens": 4,
              "token_objects": ["t/h1/m/l0/s0", "t/h1/m/l0/s1", "t/h1/m/l0/s2", "t/h1/m/l0/s3"],
              "tokens_unmatched": []},
   "statements": {
     "*": {"segments_in_window": 8, "named_in_window": 8, "name_pruned": true, "token_only_fetched": 0,
           "unrouted_fetched_expected": 8, "value_holders": 8, "expected_routed_prunes": 0},
     "mb_absent_selective_labels": {"named_in_window": 0, "token_only_fetched": 4, "unrouted_fetched_expected": 4,
                                    "holders": 0, "value_holders": 0, "expected_routed_prunes": 0},
     "mb_single_counter_rate": {"holders": 2},
     "mb_single_gauge_instant": {"holders": 2},
     "mb_selective_job_rate_sum": {"holders": 2}}},
 "records": {
   "folded-unrouted": {
     "*": {"segmentsFetched": 8, "segmentsPruned": 0, "segmentsPrunedByLabelIndex": 0},
     "mb_absent_selective_labels": {"segmentsFetched": 4, "segmentsPruned": 8}},
   "folded-routed": {
     "*": {"segmentsFetched": 8, "segmentsPruned": 0, "segmentsPrunedByLabelIndex": 0,
           "keyIndex": {"s3GetRequests": 0, "cacheHits": 4}},
     "mb_absent_selective_labels": {"segmentsFetched": 4, "segmentsPruned": 8,
                                    "keyIndex": {"s3GetRequests": 0, "cacheHits": 0}},
     "mb_single_counter_rate": {"keyIndex": {"s3GetRequests": 0, "cacheHits": 10}}}}}
JSON
MB_STAGE1=1 STUB_RUN_FIGURES="${TMP}/run-42495422937.json" run_stage1 z27 ci
expect_rc z27 0
check "z27: the absent statement expects its 4 token objects and fetched them" '[{k: s[k] for k in ("named_in_window", "token_only_fetched", "unrouted_fetched_expected", "value_holders", "expected_routed_prunes", "routed_pruned_by_label_index")} for s in j("stamps.json")["stage1"]["holders"]["statements"] if s["id"] == "mb_absent_selective_labels"] == [{"named_in_window": 0, "token_only_fetched": 4, "unrouted_fetched_expected": 4, "value_holders": 0, "expected_routed_prunes": 0, "routed_pruned_by_label_index": [0]}]'
check "z27: no window or prune mismatch, nothing unchecked, no unread leaf, 4 token objects" 'all(j("stamps.json")["stage1"]["holders"][k] == 0 for k in ("window_mismatches_count", "window_unchecked_count", "routed_prune_mismatches_count", "routed_prune_unchecked_count", "routed_leaf_unread_count")) and j("stamps.json")["stage1"]["holders"]["token_objects"] == 4 and j("stamps.json")["stage1"]["holders"]["min_commit_tokens"] == 4'
check "z27: the routed pass pruned nothing by the label index" 'j("stage1-compare.json")["passes"]["routed"]["records_pruned_by_label_index"] == 0'

# z30: a routed record whose two passes issued 7 leaf GETs over 2 covered
# parts (bound (1 + 2) x 2 = 6) with a part beyond the ceiling, and one whose
# labelIndexValues carries no whole-number figure.
cat >"${TMP}/run-values.json" <<'JSON'
{"holders": {"report": {}, "statements": {}},
 "records": {"folded-routed": {
   "mb_selective_job_rate_sum": {"labelIndexValues": [
     {"labels": ["__name__"], "valueKey8": "00112233445566aa", "leafGetRequests": 5, "leafWireBytes": 900,
      "leafCacheHits": 0, "partsCovered": 2, "partsUncovered": 0, "partsBeyondCeiling": 0, "leavesRejected": 0,
      "reservationsRefused": 0, "skippedByBudget": 0, "segmentsPruned": 1, "segmentsPrunedAlone": 0},
     {"labels": ["job"], "valueKey8": "00112233445566bb", "leafGetRequests": 2, "leafWireBytes": 300,
      "leafCacheHits": 1, "partsCovered": 2, "partsUncovered": 0, "partsBeyondCeiling": 1, "leavesRejected": 0,
      "reservationsRefused": 0, "skippedByBudget": 0, "segmentsPruned": 3, "segmentsPrunedAlone": 2}]},
   "mb_single_counter_rate": {"labelIndexValues": [{"labels": ["__name__"], "leafGetRequests": "1"}]}}}}
JSON
MB_STAGE1=1 STUB_RUN_FIGURES="${TMP}/run-values.json" run_stage1 z30 ci
expect_rc z30 0
check "z30: the over-bound record's figures, its __name__ pass pruned nothing alone" '[v for v in j("stage1-compare.json")["routed_values"] if v["entry"] == "mb_selective_job_rate_sum"] == [{"entry": "mb_selective_job_rate_sum", "rep": 1, "n": 2, "leaf_gets": 7, "leaf_cache_hits": 1, "covering_parts": 2, "leaf_gets_bound": 6, "parts_beyond_ceiling": 1, "name_pruned_alone": False}]'
check "z30: the statement over its bound, the part beyond the ceiling and the unreadable record are counted" '{k: j("stage1-compare.json")[k] for k in ("routed_records_with_values", "routed_records_values_unreadable_count", "routed_records_values_unreadable", "statements_leaf_gets_over_bound_count", "statements_leaf_gets_over_bound", "parts_beyond_ceiling_total")} == {"routed_records_with_values": len('"${CORPUS_IDS}"') - 1, "routed_records_values_unreadable_count": 1, "routed_records_values_unreadable": [{"entry": "mb_single_counter_rate", "rep": 1}], "statements_leaf_gets_over_bound_count": 1, "statements_leaf_gets_over_bound": ["mb_selective_job_rate_sum"], "parts_beyond_ceiling_total": 1} and j("stamps.json")["stage1"]["compare"]["routed_records_values_unreadable"] == ["mb_single_counter_rate"]'

# --- za: the Stage 1 arms (issue #3969) -----------------------------------------------
CONTROL_IMAGE="stub/ravel-control@sha256:$(printf 'c%.0s' {1..64})"
CONTROL_COMMIT="$(printf 'd%.0s' {1..40})"
# za: every arm on: each arm's steps after stage1-holders, in order. The
# erasure reference pass (the corpus without the subject's series, routed)
# is pruned by the label index and answers one entry differently from the
# routed pass, and the pending pass answers as the reference does.
cat >"${TMP}/run-arms.json" <<'JSON'
{"holders": {"report": {}, "statements": {}},
 "records": {"folded-erasure-reference": {"*": {"segmentsPrunedByLabelIndex": 3},
                                          "mb_selective_job_rate_sum": {"result_hash": "blake3:mb_selective_job_rate_sum-erased"}},
             "folded-erasure-pending": {"mb_selective_job_rate_sum": {"result_hash": "blake3:mb_selective_job_rate_sum-erased"}}}}
JSON
MB_STAGE1=1 MB_STAGE1_ARMS=extra-hour,l1,erasure MB_CONTROL_IMAGE="${CONTROL_IMAGE}" MB_CONTROL_SOURCE_COMMIT="${CONTROL_COMMIT}" \
  STUB_RUN_FIGURES="${TMP}/run-arms.json" run_stage1 za ci
expect_rc za 0

check "za: the arm steps follow stage1-holders in order, the control's between stage1-unrouted and stage1-leaves" 'j("stamps.json")["steps_completed"][j("stamps.json")["steps_completed"].index("stage1-fold"):] == ["stage1-fold","stage1-unrouted","stage1-control","stage1-control-restore","stage1-leaves","stage1-restart","stage1-routed","stage1-holders","stage1-extra-hour-ingest","stage1-extra-hour-fold-on","stage1-extra-hour-fold-off","stage1-extra-hour","stage1-l1-compact","stage1-l1-fold","stage1-l1","stage1-erasure-reference","stage1-erasure-submit","stage1-erasure-pending","stage1-erasure-rewrite","stage1-erasure-fold","stage1-erasure"] and j("stamps.json")["status"] == "passed"'
check "za: every arm stamped on" 'j("stamps.json")["stage1"]["arms"] == {"control": True, "extra_hour": True, "l1": True, "erasure": True}'
if python3 -I - "${LOG}" "${IMAGE}" "${CONTROL_IMAGE}" <<'PY'; then
import re, sys
lines = open(sys.argv[1]).read().splitlines()
main, control = sys.argv[2:4]
unrouted = "--disable-fold --fold-key-index-leaves"
routed = unrouted + " --fold-metrics-label-index"
want = [(unrouted, main), (unrouted, main), (unrouted, control), (unrouted, main),
        (routed, main), (routed, main), (routed, main)]
flags = [l.split("=", 1)[1] for l in lines if l.startswith("docker-env recreate ")]
images = [l.split("=", 1)[1] for l in lines if l.startswith("docker-image recreate ")]
folds = [re.search(r" catalog fold --tenant \S+ --shards \S+ (.*)$", l).group(1)
         for l in lines if l.startswith("docker compose ") and " catalog fold " in l]
leafy = "--signal metrics --writers-stopped --key-index-leaves --metrics-label-index --json"
ok = (list(zip(flags, images)) == want
      and folds[3:] == [leafy, "--signal metrics --writers-stopped --json", leafy, leafy])
if not ok:
    print(list(zip(flags, images)), folds)
sys.exit(0 if ok else 1)
PY
  pass "za: seven recreates, the control's alone on the control image, and the arm folds with their listed flags, the leaf-on extra-hour fold first"
else
  fail "za: the recreates or the arm folds are not the listed ones"
fi
if python3 -I - "${LOG}" <<'PY'; then
import re, sys
lines = open(sys.argv[1]).read().splitlines()
def first(pred):
    return next((i for i, l in enumerate(lines) if pred(l)), -1)
ingest = first(lambda l: l.startswith("metricsbench_ingest ") and " --steps " in l)
def cp(l):
    m = re.search(r" s3 cp (?:--recursive )?(\S+) (\S+)", l)
    return (m.group(1), m.group(2)) if m else ("", "")
def save(name):
    return first(lambda l: cp(l)[0].startswith("s3://") and cp(l)[1].startswith(f"/stage1-s3/{name}/"))
def restore(name):
    return first(lambda l: cp(l)[0].startswith(f"/stage1-s3/{name}/") and cp(l)[1].startswith("s3://"))
def fold(leafy, after):
    return next((i for i, l in enumerate(lines) if i > after and " catalog fold " in l and " --writers-stopped " in l
                 and (" --key-index-leaves " in l) == leafy), -1)
rebuild = first(lambda l: " --rebuild-key-index " in l)
fold_on = fold(True, ingest)
fold_off = fold(False, ingest)
sleep = next((i for i, l in enumerate(lines) if i > fold_off and l.startswith("stubsleep ")), -1)
compact = first(lambda l: " compact-tenant " in l)
l1 = first(lambda l: "--pass-label folded-l1" in l)
submit = first(lambda l: " erase submit " in l and "--matcher instance=metricsbench-instance-0" in l)
pending = first(lambda l: "--pass-label folded-erasure-pending" in l)
maintain = first(lambda l: " --mode maintain " in l)
status_ = first(lambda l: " erase status " in l)
order = dict(save_leafless=save("head-leafless"), rebuild=rebuild, save_routed=save("head-routed"), ingest=ingest,
             restore_routed=restore("head-routed"), fold_on=fold_on, save_fold_on=save("head-fold-on"),
             restore_leafless=restore("head-leafless"), fold_off=fold_off, restore_fold_on=restore("head-fold-on"),
             sleep=sleep, compact=compact, l1=l1, submit=submit, pending=pending, maintain=maintain, status=status_)
ok = list(order.values()) == sorted(order.values()) and min(order.values()) >= 0
if not ok:
    print(order)
sys.exit(0 if ok else 1)
PY
  pass "za: leafless HEAD saved, the leaf rebuild, routed HEAD saved, extra-hour ingest, routed HEAD put back, the leaf-on fold, its HEAD saved, leafless HEAD put back, the leaf-off fold, the leaf-on HEAD put back, the wait for the seal, compact-tenant, the l1 pass, erase submit, the pending pass, the maintain container, erase status"
else
  fail "za: the arm calls are not in their order"
fi
check "za: the leaf-on fold started from the routed HEAD, the leaf-off fold from the leafless one with no switch-off over leaves, both from the same watermark, the leaf build only in the leaf-on fold" 'j("stamps.json")["stage1"]["extra_hour"]["start_mismatches"] == 0 and set(j("stamps.json")["stage1"]["extra_hour"]["head_sha256"]) == {"fold_off_start", "fold_on_start", "after"} and len(set(j("stamps.json")["stage1"]["extra_hour"]["head_sha256"].values())) == 3 and j("stamps.json")["stage1"]["extra_hour"]["fold_off"]["key_index_switch_off_over_leaves"] is False and j("stamps.json")["stage1"]["extra_hour"]["fold_off"]["put_requests"] == 3 == j("stamps.json")["stage1"]["extra_hour"]["fold_on"]["put_requests"] and j("stamps.json")["stage1"]["extra_hour_fold_off"]["key_index_leaves_written"] == 0 and j("stamps.json")["stage1"]["extra_hour_fold_on"]["key_index_leaves_written"] == 2 and j("stamps.json")["stage1"]["extra_hour"]["watermark_hour"] == 490002 and "wall_ratio_on_off errs high" in j("stamps.json")["stage1"]["extra_hour"]["method"]'
check "za: the extra hour's tail objects, leaf bytes over the bytes of the objects its tail parts cover and its one covered tail part" '{k: j("stamps.json")["stage1"]["extra_hour"][k] for k in ("tail_objects", "tail_data_bytes", "leaf_bytes", "leaf_bytes_denominator_objects", "leaf_bytes_denominator", "leaf_bytes_ratio", "tail_parts", "uncovered_tail_parts_count")} == {"tail_objects": 30, "tail_data_bytes": 3000, "leaf_bytes": 10, "leaf_bytes_denominator_objects": 30, "leaf_bytes_denominator": 3000, "tail_parts": 1, "leaf_bytes_ratio": 10 / 3000, "uncovered_tail_parts_count": 0}'
check "za: the control pass ran on the control image and compares GETs, bytes and latency per statement" 'j("stamps.json")["stage1"]["control"]["fold_state"].endswith("; image '"${CONTROL_IMAGE}"'") and j("stamps.json")["stage1"]["control_image"]["control"]["source_commit"] == "'"${CONTROL_COMMIT}"'" and {k: j("stage1-control-compare.json")[k] for k in ("statements_compared", "statements_more_gets_count", "statements_more_bytes_count", "statements_unchecked_count", "statements_slower_count", "latency_ratio_max")} == {"statements_compared": len('"${CORPUS_IDS}"'), "statements_more_gets_count": 0, "statements_more_bytes_count": 0, "statements_unchecked_count": 0, "statements_slower_count": 0, "latency_ratio_max": 1.0}'
check "za: compact-tenant's counters, the L1 objects listed and the l1 pass equal to the routed one" 'j("stamps.json")["stage1"]["l1_compact"]["compacted"] == 2 and j("stamps.json")["stage1"]["l1_compact"]["failed"] == 0 and j("stamps.json")["stage1"]["l1_objects"]["l1_objects"] == 100 and j("stage1-l1-compare.json")["entries_compared"] == len('"${CORPUS_IDS}"') == j("stage1-l1-compare.json")["entries_equal"]'
check "za: the erasure names its subject, the rewrite's state and path, one rewritten part with no stale leaf, and leaves the sweep unchecked" 'j("stamps.json")["stage1"]["erasure_submit"]["matcher"] == "instance=\"metricsbench-instance-0\"" and j("stamps.json")["stage1"]["erasure_rewrite"]["state"] == "COMPLETED" and j("stamps.json")["stage1"]["erasure_rewrite"]["deferral_cause"] == "none (completed cleanly)" and "dropped_total" not in j("stamps.json")["stage1"]["erasure_rewrite"] and j("stamps.json")["stage1"]["erasure_rewrite"]["path"].startswith("a one-off ravel-server --mode maintain container") and {k: j("stamps.json")["stage1"]["erasure_leaves"][k] for k in ("parts_before", "parts_after", "rewritten_parts_count", "stale_leaves_count")} == {"parts_before": 4, "parts_after": 4, "rewritten_parts_count": 1, "stale_leaves_count": 0} and j("stamps.json")["stage1"]["erasure_leaves"]["rewritten_parts"][0]["key"] == "t/h1/m/catalog/part-0" and j("stamps.json")["stage1"]["erasure_sweep"].startswith("not checked: ") and j("stage1-erasure-compare.json")["base"] == "erasure_reference" and j("stage1-erasure-compare.json")["other"] == "erasure_pending" and j("stage1-erasure-compare.json")["entries_differing_count"] == 0'
check "za: the subject compare: the reference pass pruned by the label index, one entry answered without the subject's series" 'j("stage1-erasure-subject.json")["base"] == "routed" and j("stage1-erasure-subject.json")["other"] == "erasure_reference" and j("stage1-erasure-subject.json")["other_records_pruned_by_label_index"] == len('"${CORPUS_IDS}"') and j("stage1-erasure-subject.json")["entries_differing_count"] == 1'
check "za: the seal wait and the maintain container's flags are stamped" 'set(j("stamps.json")["stage1"]["erasure_seal_wait"]) == {"submitted_unix_s", "deadline_unix_s", "sleep_s", "waited_s", "basis"} and j("stamps.json")["stage1"]["erasure_seal_wait"]["deadline_unix_s"] == (j("stamps.json")["stage1"]["erasure_seal_wait"]["submitted_unix_s"] // 3600 + 1) * 3600 + 3900 and j("stamps.json")["stage1"]["erasure_maintain"].startswith("ravel-server --mode maintain --store s3 ") and "--maintain-key-file /maintain.key" in j("stamps.json")["stage1"]["erasure_maintain"] and "no ravel-server flag turns compaction off" in j("stamps.json")["stage1"]["erasure_maintain"]'
check "za: l1 is stamped not measured" 'isinstance(j("stamps.json")["stage1"]["l1_not_measured"], str) and j("stamps.json")["stage1"]["l1_not_measured"] != ""'
if key_mount="$(grep '^docker compose .* --mode maintain ' "${LOG}" | grep -oE -- '-v [^ ]+:/maintain.key:ro' | sed -e 's/^-v //' -e 's/:\/maintain.key:ro$//')" &&
  [ -n "${key_mount}" ] && [ "${key_mount#"${OUT}"}" = "${key_mount}" ] && [ ! -e "${key_mount}" ]; then
  pass "za: the maintain record key lives outside the out dir and is gone once the step ends"
else
  fail "za: the maintain record key mount '${key_mount:-}' is under ${OUT} or still exists"
fi

# zb-ze: each arm alone runs its own steps only, with figures its bands
# reject (the band checks are under stage1-check.sh below). zb: a control
# image whose records issue fewer GETs and bytes and answer in 1 ms.
cat >"${TMP}/run-control.json" <<'JSON'
{"holders": {"report": {}, "statements": {}},
 "records": {"folded-control": {"*": {"latency_ms": 1, "phases": [
   {"name": "scan", "requests": 1, "phase": "scan", "s3GetRequests": 0, "s3GetWireBytes": 50}]}}}}
JSON
MB_STAGE1=1 MB_CONTROL_IMAGE="${CONTROL_IMAGE}" MB_CONTROL_SOURCE_COMMIT="${CONTROL_COMMIT}" \
  STUB_RUN_FIGURES="${TMP}/run-control.json" run_stage1 zb ci
expect_rc zb 0
check "zb: the control's steps alone, between stage1-unrouted and stage1-leaves" 'j("stamps.json")["steps_completed"][-7:] == ["stage1-unrouted","stage1-control","stage1-control-restore","stage1-leaves","stage1-restart","stage1-routed","stage1-holders"] and j("stamps.json")["stage1"]["arms"] == {"control": True, "extra_hour": False, "l1": False, "erasure": False}'
check "zb: every statement 12.5x the control's latency with no spread is slower" 'j("stage1-control-compare.json")["statements_slower_count"] == len('"${CORPUS_IDS}"') and j("stage1-control-compare.json")["latency_ratio_max"] == 12.5 and j("stage1-control-compare.json")["entries"][0]["control"]["latency_ms_spread"] == 0'
# zb2: the unrouted median 12.5 ms is about 6% above the control's 11.8 ms,
# whose two reps (11.0 and 12.6 ms) spread wider than that excess.
cat >"${TMP}/run-control-spread.json" <<'JSON'
{"holders": {"report": {}, "statements": {}},
 "records": {"folded-control": {"*": {"latency_ms": 11.0}}},
 "rep2": {"folded-control": {"*": {"latency_ms": 12.6}}}}
JSON
MB_STAGE1=1 MB_CONTROL_IMAGE="${CONTROL_IMAGE}" MB_CONTROL_SOURCE_COMMIT="${CONTROL_COMMIT}" \
  STUB_RUN_FIGURES="${TMP}/run-control-spread.json" run_stage1 zb2 ci
expect_rc zb2 0
check "zb2: a 6% excess inside the control's spread is not slower" 'j("stage1-control-compare.json")["statements_slower_count"] == 0 and abs(j("stage1-control-compare.json")["entries"][0]["control"]["latency_ms_spread"] - 1.6) < 1e-9 and abs(j("stage1-control-compare.json")["latency_ratio_max"] - 12.5 / 11.8) < 1e-9'
# zc: an extra hour whose leaves are a third of its data bytes, on a tail
# part whose leaf leaves an entry uncovered.
MB_STAGE1=1 MB_STAGE1_ARMS=extra-hour STUB_EXTRA_LEAF_BYTES=1000 STUB_EXTRA_UNCOVERED=1 run_stage1 zc ci
expect_rc zc 0
check "zc: the extra hour's steps alone, after stage1-holders" 'j("stamps.json")["steps_completed"][-5:] == ["stage1-holders","stage1-extra-hour-ingest","stage1-extra-hour-fold-on","stage1-extra-hour-fold-off","stage1-extra-hour"] and j("stamps.json")["stage1"]["arms"] == {"control": False, "extra_hour": True, "l1": False, "erasure": False}'
check "zc: the uncovered tail part is named" 'j("stamps.json")["stage1"]["extra_hour"]["uncovered_tail_parts_count"] == 1 and j("stamps.json")["stage1"]["extra_hour"]["leaf_bytes_ratio"] == 1000 / 3000'
# zc2: a tail part whose range reaches back over every ingest hour: the
# leaves index all 300 objects (30000 bytes), not only the 30 new ones.
MB_STAGE1=1 MB_STAGE1_ARMS=extra-hour STUB_TAIL_SPAN=1 STUB_EXTRA_LEAF_BYTES=200 run_stage1 zc2 ci
expect_rc zc2 0
check "zc2: the leaf bytes ratio is over the union of the objects the tail part covers" '{k: j("stamps.json")["stage1"]["extra_hour"][k] for k in ("tail_objects", "tail_data_bytes", "leaf_bytes", "leaf_bytes_denominator_objects", "leaf_bytes_denominator", "leaf_bytes_ratio")} == {"tail_objects": 30, "tail_data_bytes": 3000, "leaf_bytes": 200, "leaf_bytes_denominator_objects": 300, "leaf_bytes_denominator": 30000, "leaf_bytes_ratio": 200 / 30000}'
# zd: an L1 pass whose hash differs on one entry and that failed on another.
cat >"${TMP}/run-l1.json" <<'JSON'
{"holders": {"report": {}, "statements": {}},
 "records": {"folded-l1": {
   "mb_selective_job_rate_sum": {"result_hash": "blake3:mb_selective_job_rate_sum-l1"},
   "mb_single_counter_rate": {"status": "failed: stub", "result_hash": null}}}}
JSON
MB_STAGE1=1 MB_STAGE1_ARMS=l1 STUB_RUN_FIGURES="${TMP}/run-l1.json" run_stage1 zd ci
expect_rc zd 0
check "zd: the l1 steps alone, after stage1-holders, with compact-tenant waiting for the seal" 'j("stamps.json")["steps_completed"][-4:] == ["stage1-holders","stage1-l1-compact","stage1-l1-fold","stage1-l1"] and j("stamps.json")["stage1"]["arms"] == {"control": False, "extra_hour": False, "l1": True, "erasure": False}'
expect_log "zd: the wait before compact-tenant goes through the sleep" "stubsleep "
# ze: an erasure-pending pass the label index pruned, and a post-erasure
# snapshot that still references the pre-erasure leaves.
cat >"${TMP}/run-erasure.json" <<'JSON'
{"holders": {"report": {}, "statements": {}},
 "records": {"folded-erasure-pending": {"*": {"segmentsPrunedByLabelIndex": 2}}}}
JSON
MB_STAGE1=1 MB_STAGE1_ARMS=erasure STUB_ERASURE_STALE=1 STUB_RUN_FIGURES="${TMP}/run-erasure.json" run_stage1 ze ci
expect_rc ze 0
check "ze: the erasure steps alone, after stage1-holders" 'j("stamps.json")["steps_completed"][-7:] == ["stage1-holders","stage1-erasure-reference","stage1-erasure-submit","stage1-erasure-pending","stage1-erasure-rewrite","stage1-erasure-fold","stage1-erasure"] and j("stamps.json")["stage1"]["arms"] == {"control": False, "extra_hour": False, "l1": False, "erasure": True}'
check "ze: the rewritten part still referencing its pre-erasure leaf is named" 'j("stamps.json")["stage1"]["erasure_leaves"]["rewritten_parts_count"] == 1 and j("stamps.json")["stage1"]["erasure_leaves"]["stale_leaves"] == [{"part_before": "t/h1/m/catalog/part-0", "part_after": "t/h1/m/catalog/part-0-e", "leaf": "t/h1/m/ki/0-v", "field": "label_value"}]'

# zf: the switches are refused before anything starts.
MB_STAGE1=1 MB_CONTROL_IMAGE="stub/ravel-control:latest" MB_CONTROL_SOURCE_COMMIT="${CONTROL_COMMIT}" run_stage1 zf1 ci
expect_rc zf1 65
expect_err "zf1: a control image with no digest is refused" "refused: MB_CONTROL_IMAGE 'stub/ravel-control:latest' carries no @sha256: digest"
expect_no_log "zf1: nothing brought up" "ravel.yml up -d"
MB_STAGE1=1 MB_CONTROL_IMAGE="${CONTROL_IMAGE}" run_stage1 zf2 ci
expect_rc zf2 64
expect_err "zf2: the control image needs its source commit" "MB_CONTROL_IMAGE and MB_CONTROL_SOURCE_COMMIT are given together or not at all"
MB_STAGE1_ARMS=l1 run_stage1 zf3 ci
expect_rc zf3 64
expect_err "zf3: the arms need MB_STAGE1=1" "MB_STAGE1_ARMS adds Stage 1 arms and needs MB_STAGE1=1"
MB_STAGE1=1 MB_STAGE1_ARMS=l1,compaction run_stage1 zf4 ci
expect_rc zf4 64
expect_err "zf4: an unknown arm is named" "MB_STAGE1_ARMS takes a comma list of extra-hour, l1 and erasure, got 'compaction'"
expect_no_log "zf4: nothing brought up" "ravel.yml up -d"

# zg: a routed HEAD that does not read back as saved refuses the leaf-on fold.
MB_STAGE1=1 MB_STAGE1_ARMS=extra-hour STUB_HEAD_RESTORE_BROKEN=1 run_stage1 zg ci
expect_rc zg 70
expect_err "zg: the refusal names the step, the HEAD and both digests" "step stage1-extra-hour-fold-on failed: the restored head-routed HEAD reads back as sha256 "
check "zg: the extra hour is stamped refused, neither fold ran" 'j("stamps.json")["stage1"]["extra_hour"].startswith("refused: the restored head-routed HEAD ") and not any(s.startswith("stage1-extra-hour-fold") for s in j("stamps.json")["steps_completed"])'
expect_no_log "zg: no extra-hour fold ran" "--writers-stopped --key-index-leaves"

# zg2: a leaf-off fold that reports a switch-off over leaves did not start
# from a leafless lineage and is refused.
MB_STAGE1=1 MB_STAGE1_ARMS=extra-hour STUB_SWITCH_OFF_OVER_LEAVES=1 run_stage1 zg2 ci
expect_rc zg2 70
expect_err "zg2: the refusal names the step and the flag" "step stage1-extra-hour failed: the leaf-off fold reports key_index_switch_off_over_leaves True, want false"
check "zg2: the extra hour is stamped refused after both folds" 'j("stamps.json")["stage1"]["extra_hour"] == "refused: the leaf-off fold reports key_index_switch_off_over_leaves True, so it did not start from a leafless lineage" and j("stamps.json")["steps_completed"][-2:] == ["stage1-extra-hour-fold-on", "stage1-extra-hour-fold-off"]'

# zh: an erasure still pending after every status poll ends the run at the rewrite.
MB_STAGE1=1 MB_STAGE1_ARMS=erasure STUB_ERASE_PENDING=1 METRICSBENCH_ERASURE_TRIES=2 run_stage1 zh ci
expect_rc zh 70
expect_err "zh: the failure names the step and the tries" "step stage1-erasure-rewrite failed: erasure "
expect_err "zh: the tries are counted" " not COMPLETED after 2 tries 15s apart"
check "zh: the steps stop before the rewrite" 'j("stamps.json")["steps_completed"][-1] == "stage1-erasure-pending"'
if key_mount="$(grep '^docker compose .* --mode maintain ' "${LOG}" | grep -oE -- '-v [^ ]+:/maintain.key:ro' | sed -e 's/^-v //' -e 's/:\/maintain.key:ro$//')" &&
  [ -n "${key_mount}" ] && [ "${key_mount#"${OUT}"}" = "${key_mount}" ] && [ ! -e "${key_mount}" ]; then
  pass "zh: the maintain record key lives outside the out dir and is gone on the failure path too"
else
  fail "zh: the maintain record key mount '${key_mount:-}' is under ${OUT} or still exists"
fi

# zi: an erasure submitted mid-hour (the stubbed clock stops at
# 1764001800, half past hour 490000) sleeps to that hour's end plus
# max_flush_lifetime plus the clock-skew allowance before the first poll.
MB_STAGE1=1 MB_STAGE1_ARMS=erasure STUB_CLOCK_AT_SUBMIT=1764001800 run_stage1 zi ci
expect_rc zi 0
check "zi: the seal wait is stamped: deadline 490001 * 3600 + 3600 + 300, the whole 5700 s slept" '{k: v for k, v in j("stamps.json")["stage1"]["erasure_seal_wait"].items() if k != "basis"} == {"submitted_unix_s": 1764001800, "deadline_unix_s": 1764007500, "sleep_s": 5700, "waited_s": 0}'
if python3 -I - "${LOG}" <<'PY'; then
import sys
lines = open(sys.argv[1]).read().splitlines()
def first(pred):
    return next((i for i, l in enumerate(lines) if pred(l)), -1)
pending = first(lambda l: "--pass-label folded-erasure-pending" in l)
sleep = first(lambda l: l == "stubsleep 5700")
maintain = first(lambda l: " --mode maintain " in l)
status_ = first(lambda l: " erase status " in l)
ok = 0 <= pending < sleep < maintain < status_
if not ok:
    print(dict(pending=pending, sleep=sleep, maintain=maintain, status=status_))
sys.exit(0 if ok else 1)
PY
  pass "zi: the pending pass, then the 5700 s sleep, then the maintain container and the first erase status"
else
  fail "zi: the seal wait is not between the pending pass and the first poll"
fi

# zj: an erasure whose fold leaves every part as it was rewrote nothing.
MB_STAGE1=1 MB_STAGE1_ARMS=erasure STUB_ERASURE_UNCHANGED=1 STUB_RUN_FIGURES="${TMP}/run-arms.json" run_stage1 zj ci
expect_rc zj 0
check "zj: no rewritten part, so no stale leaf either" 'j("stamps.json")["stage1"]["erasure_leaves"]["rewritten_parts_count"] == 0 and j("stamps.json")["stage1"]["erasure_leaves"]["stale_leaves_count"] == 0'

# za1: no arm switch: the arm steps do not run and each arm's stamps say so.
OUT="${TMP}/z1/out"
check "za1 (z1's run): no arm step ran, every arm stamped off" 'not any(s.startswith(("stage1-control", "stage1-extra-hour", "stage1-l1", "stage1-erasure")) for s in j("stamps.json")["steps_completed"]) and j("stamps.json")["stage1"]["arms"] == {"control": False, "extra_hour": False, "l1": False, "erasure": False} and all(j("stamps.json")["stage1"][k].startswith("skipped: ") for k in ("control_compare", "extra_hour", "l1_compare", "erasure_compare"))'

# z19: the scrape before the query step fails: the records say unknown and why.
mapfile -t args < <(full_args "${TMP}/z19/out")
STUB_METRICS_DOWN_AT=1 run_case z19 "${args[@]}"
expect_rc z19 0
OUT="${TMP}/z19/out"
expect_log "z19: the query step is told the state is unknown" "--pass-label unknown --fold-state unknown: the /metrics scrape before the query step"
check "z19: every record says unknown" 'all(r["pass_label"] == "unknown" and r["fold_state"].startswith("unknown: the /metrics scrape before the query step") for r in j("diagnostics.json")["ravel_cost"])'

# --- z: stage1-check.sh ------------------------------------------------------------------
CHECK="${HERE}/../stage1-check.sh"
CI_BANDS="${HERE}/../stage1-bands.ci.json"
# run_check NAME BANDS DIR: rc, CHECK_OUT and CHECK_ERR.
run_check() {
  CHECK_OUT="${TMP}/$1.check.out"
  CHECK_ERR="${TMP}/$1.check.err"
  rc=0
  bash "${CHECK}" "$2" "$3" >"${CHECK_OUT}" 2>"${CHECK_ERR}" || rc=$?
}
expect_check_err() {
  if grep -qxF -- "$2" "${CHECK_ERR}"; then pass "$1"; else fail "$1 (no line '$2' in stderr: $(cat "${CHECK_ERR}"))"; fi
}
FIGURES="$(python3 -I -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["figures"]))' "${CI_BANDS}")"
ARM_FIGURES="$(python3 -I -c 'import json,sys; print(sum("when" in f for f in json.load(open(sys.argv[1]))["figures"]))' "${CI_BANDS}")"

# Clean: the shipped ci band file against case z1's run, every arm's figure n/a.
run_check clean "${CI_BANDS}" "${TMP}/z1/out"
if [ "${rc}" -eq 0 ] && [ "$(wc -l <"${CHECK_OUT}")" -eq "${FIGURES}" ] && [ "$(grep -c '^ok ' "${CHECK_OUT}")" -eq $((FIGURES - ARM_FIGURES)) ] &&
  [ "$(grep -c '^n/a ' "${CHECK_OUT}")" -eq "${ARM_FIGURES}" ] && [ "${ARM_FIGURES}" -eq 23 ] &&
  grep -qxF 'n/a l1_objects: stage1.arms.l1 is false, not true (stamps.json stage1.l1_objects.l1_objects)' "${CHECK_OUT}" &&
  grep -qxF 'ok leaves_parts_rebuilt = 3 in [1, null] (stamps.json stage1.leaves.parts_rebuilt)' "${CHECK_OUT}" &&
  grep -qxF 'ok unrouted_records_pruned_by_label_index = 0 in [0, 0] (stage1-compare.json passes.unrouted.records_pruned_by_label_index)' "${CHECK_OUT}" &&
  grep -qxF 'ok holders_selective_job_rate_sum = 2 in [1, null] (holders.json statements[id=mb_selective_job_rate_sum].holders)' "${CHECK_OUT}"; then
  pass "check clean: exit 0, one ok line per figure (${FIGURES}) but the ${ARM_FIGURES} arm figures, n/a with every arm off"
else
  fail "check clean: exit ${rc}, stdout $(cat "${CHECK_OUT}"), stderr $(cat "${CHECK_ERR}")"
fi

# The arms' bands: every figure in band on case za's run, every arm on.
run_check arms "${CI_BANDS}" "${TMP}/za/out"
if [ "${rc}" -eq 0 ] && [ "$(grep -c '^ok ' "${CHECK_OUT}")" -eq "${FIGURES}" ] &&
  grep -qxF 'ok extra_hour_leaf_bytes_ratio = 0.0033333333333333335 in [0, 0.01] (stamps.json stage1.extra_hour.leaf_bytes_ratio)' "${CHECK_OUT}" &&
  grep -qxF 'ok extra_hour_tail_parts = 1 in [1, null] (stamps.json stage1.extra_hour.tail_parts)' "${CHECK_OUT}" &&
  grep -qxF 'ok control_statements_slower = 0 in [0, 0] (stage1-control-compare.json statements_slower_count)' "${CHECK_OUT}" &&
  grep -qxF 'ok erasure_subject_pruned_by_label_index = 24 in [1, null] (stage1-erasure-subject.json other_records_pruned_by_label_index)' "${CHECK_OUT}" &&
  grep -qxF 'ok erasure_subject_entries_differing = 1 in [1, null] (stage1-erasure-subject.json entries_differing_count)' "${CHECK_OUT}" &&
  grep -qxF 'ok erasure_pending_status_unexpected = 0 in [0, 0] (stage1-erasure-compare.json other_records_status_unexpected)' "${CHECK_OUT}" &&
  grep -qxF 'ok erasure_pending_entries_without_hash = 0 in [0, 0] (stage1-erasure-compare.json entries_without_hash_count)' "${CHECK_OUT}" &&
  grep -qxF 'ok erasure_rewritten_parts = 1 in [1, null] (stamps.json stage1.erasure_leaves.rewritten_parts_count)' "${CHECK_OUT}" &&
  grep -qxF 'ok erasure_stale_leaves = 0 in [0, 0] (stamps.json stage1.erasure_leaves.stale_leaves_count)' "${CHECK_OUT}" &&
  ! grep -q 'dropped_total\|fetch_memory' "${CHECK_OUT}"; then
  pass "check arms: exit 0, every figure ok on za's run"
else
  fail "check arms: exit ${rc}, stdout $(cat "${CHECK_OUT}"), stderr $(cat "${CHECK_ERR}")"
fi
# Each arm's failing case: zb's control, zc's extra hour, zd's l1, ze's erasure.
run_check arm-control "${CI_BANDS}" "${TMP}/zb/out"
expect_rc "check control" 1
expect_check_err "check control: more GETs than the control" "out of band control_statements_more_gets = 24 not in [0, 0] (stage1-control-compare.json statements_more_gets_count)"
expect_check_err "check control: more bytes than the control" "out of band control_statements_more_bytes = 24 not in [0, 0] (stage1-control-compare.json statements_more_bytes_count)"
expect_check_err "check control: every statement slower" "out of band control_statements_slower = 24 not in [0, 0] (stage1-control-compare.json statements_slower_count)"
expect_check_err "check control: three failures counted" "stage1-check: 3 of ${FIGURES} figures failed"
run_check arm-control-spread "${CI_BANDS}" "${TMP}/zb2/out"
if [ "${rc}" -eq 0 ] &&
  grep -qxF 'ok control_statements_slower = 0 in [0, 0] (stage1-control-compare.json statements_slower_count)' "${CHECK_OUT}"; then
  pass "check control spread: zb2's 6% excess inside the spread passes"
else
  fail "check control spread: exit ${rc}, stderr $(cat "${CHECK_ERR}")"
fi
run_check arm-extra-hour-span "${CI_BANDS}" "${TMP}/zc2/out"
if [ "${rc}" -eq 0 ] &&
  grep -qxF 'ok extra_hour_leaf_bytes_ratio = 0.006666666666666667 in [0, 0.01] (stamps.json stage1.extra_hour.leaf_bytes_ratio)' "${CHECK_OUT}"; then
  pass "check extra hour span: zc2's 200 leaf bytes over the 30000 bytes the tail part covers pass"
else
  fail "check extra hour span: exit ${rc}, stderr $(cat "${CHECK_ERR}")"
fi
run_check arm-extra-hour "${CI_BANDS}" "${TMP}/zc/out"
expect_rc "check extra hour" 1
expect_check_err "check extra hour: the leaf bytes ratio" "out of band extra_hour_leaf_bytes_ratio = 0.3333333333333333 not in [0, 0.01] (stamps.json stage1.extra_hour.leaf_bytes_ratio)"
expect_check_err "check extra hour: the uncovered tail part" "out of band extra_hour_uncovered_tail_parts = 1 not in [0, 0] (stamps.json stage1.extra_hour.uncovered_tail_parts_count)"
expect_check_err "check extra hour: two failures counted" "stage1-check: 2 of ${FIGURES} figures failed"
run_check arm-l1 "${CI_BANDS}" "${TMP}/zd/out"
expect_rc "check l1" 1
expect_check_err "check l1: the differing hash" "out of band l1_entries_differing = 1 not in [0, 0] (stage1-l1-compare.json entries_differing_count)"
expect_check_err "check l1: the failed status" "out of band l1_records_status_unexpected = 1 not in [0, 0] (stage1-l1-compare.json other_records_status_unexpected)"
expect_check_err "check l1: two failures counted" "stage1-check: 2 of ${FIGURES} figures failed"
run_check arm-erasure "${CI_BANDS}" "${TMP}/ze/out"
expect_rc "check erasure" 1
expect_check_err "check erasure: a pending record the label index pruned" "out of band erasure_pending_pruned_by_label_index = 24 not in [0, 0] (stage1-erasure-compare.json other_records_pruned_by_label_index)"
expect_check_err "check erasure: the stale leaf" "out of band erasure_stale_leaves = 1 not in [0, 0] (stamps.json stage1.erasure_leaves.stale_leaves_count)"
expect_check_err "check erasure: a reference pass the label index did not prune" "out of band erasure_subject_pruned_by_label_index = 0 not in [1, null] (stage1-erasure-subject.json other_records_pruned_by_label_index)"
expect_check_err "check erasure: a reference pass answering as the routed one" "out of band erasure_subject_entries_differing = 0 not in [1, null] (stage1-erasure-subject.json entries_differing_count)"
expect_check_err "check erasure: four failures counted" "stage1-check: 4 of ${FIGURES} figures failed"
run_check arm-erasure-unchanged "${CI_BANDS}" "${TMP}/zj/out"
expect_rc "check erasure unchanged" 1
expect_check_err "check erasure unchanged: no rewritten part" "out of band erasure_rewritten_parts = 0 not in [1, null] (stamps.json stage1.erasure_leaves.rewritten_parts_count)"
expect_check_err "check erasure unchanged: one failure counted" "stage1-check: 1 of ${FIGURES} figures failed"
# The arm figures no stub case moves out of band, set out of band in a copy of za's run.
cp -R "${TMP}/za/out" "${TMP}/check-arms"
python3 -I - "${TMP}/check-arms" <<'PY'
import json, os, sys
d = sys.argv[1]
def edit(name, fn):
    p = os.path.join(d, name)
    doc = json.load(open(p))
    fn(doc)
    json.dump(doc, open(p, "w"))
def stamps(doc):
    s = doc["stage1"]
    s["extra_hour"].update(tail_objects=0, start_mismatches=1, wall_ratio_on_off=11, tail_parts=0)
    s["l1_objects"]["l1_objects"] = 0
    s["erasure_leaves"]["rewritten_parts_count"] = 0
edit("stamps.json", stamps)
edit("stage1-control-compare.json", lambda c: c.update(statements_unchecked_count=1))
edit("stage1-l1-compare.json", lambda c: c.update(entries_compared=0))
edit("stage1-erasure-compare.json", lambda c: c.update(entries_compared=0, entries_differing_count=1,
                                                       other_records_status_unexpected=1, entries_without_hash_count=1))
PY
run_check arms-edited "${CI_BANDS}" "${TMP}/check-arms"
expect_rc "check arm figures" 1
expect_check_err "check arm figures: an unchecked control statement" "out of band control_statements_unchecked = 1 not in [0, 0] (stage1-control-compare.json statements_unchecked_count)"
expect_check_err "check arm figures: no tail object" "out of band extra_hour_tail_objects = 0 not in [1, null] (stamps.json stage1.extra_hour.tail_objects)"
expect_check_err "check arm figures: the folds' start HEADs differ" "out of band extra_hour_start_mismatches = 1 not in [0, 0] (stamps.json stage1.extra_hour.start_mismatches)"
expect_check_err "check arm figures: the leaf build's wall ratio" "out of band extra_hour_wall_ratio_on_off = 11 not in [0, 10] (stamps.json stage1.extra_hour.wall_ratio_on_off)"
expect_check_err "check arm figures: no L1 object" "out of band l1_objects = 0 not in [1, null] (stamps.json stage1.l1_objects.l1_objects)"
expect_check_err "check arm figures: no l1 entry compared" "out of band l1_entries_compared = 0 not in [1, null] (stage1-l1-compare.json entries_compared)"
expect_check_err "check arm figures: a pending hash differing" "out of band erasure_pending_entries_differing = 1 not in [0, 0] (stage1-erasure-compare.json entries_differing_count)"
expect_check_err "check arm figures: no pending entry compared" "out of band erasure_pending_entries_compared = 0 not in [1, null] (stage1-erasure-compare.json entries_compared)"
expect_check_err "check arm figures: no tail part" "out of band extra_hour_tail_parts = 0 not in [1, null] (stamps.json stage1.extra_hour.tail_parts)"
expect_check_err "check arm figures: a pending status other than ok or incorrect" "out of band erasure_pending_status_unexpected = 1 not in [0, 0] (stage1-erasure-compare.json other_records_status_unexpected)"
expect_check_err "check arm figures: a pending entry without a hash" "out of band erasure_pending_entries_without_hash = 1 not in [0, 0] (stage1-erasure-compare.json entries_without_hash_count)"
expect_check_err "check arm figures: no rewritten part" "out of band erasure_rewritten_parts = 0 not in [1, null] (stamps.json stage1.erasure_leaves.rewritten_parts_count)"
expect_check_err "check arm figures: twelve failures counted" "stage1-check: 12 of ${FIGURES} figures failed"

# Out of band: case z2's differing hash.
run_check oob "${CI_BANDS}" "${TMP}/z2/out"
expect_rc "check out-of-band" 1
expect_check_err "check out-of-band: the figure, value and band named" "out of band entries_differing = 1 not in [0, 0] (stage1-compare.json entries_differing_count)"
expect_check_err "check out-of-band: one failure counted" "stage1-check: 1 of ${FIGURES} figures failed"

# Out of band: case z2b's multi-hash entry fails its own band, not entries_differing's.
run_check oob-multi "${CI_BANDS}" "${TMP}/z2b/out"
expect_rc "check out-of-band multi-hash" 1
expect_check_err "check out-of-band multi-hash: the figure, value and band named" "out of band entries_multi_hash = 1 not in [0, 0] (stage1-compare.json entries_multi_hash_count)"
expect_check_err "check out-of-band multi-hash: one failure counted" "stage1-check: 1 of ${FIGURES} figures failed"

# Missing: z1's run without holders.json, and with a stage1 stamp that is a failure string.
cp -R "${TMP}/z1/out" "${TMP}/check-missing"
rm "${TMP}/check-missing/holders.json"
python3 -I - "${TMP}/check-missing/stamps.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d["stage1"]["leaves"] = "failed: ravel-cli catalog fold exited 1"
json.dump(d, open(sys.argv[1], "w"))
PY
run_check missing "${CI_BANDS}" "${TMP}/check-missing"
expect_rc "check missing" 1
expect_check_err "check missing: an absent file names the figure" "missing holders_selective_job_rate_sum: holders.json does not exist (holders.json statements[id=mb_selective_job_rate_sum].holders)"
expect_check_err "check missing: a failed stamp is not a figure" 'missing leaves_parts_rebuilt: stage1.leaves is str "failed: ravel-cli catalog fold exited 1", not an object (stamps.json stage1.leaves.parts_rebuilt)'
expect_check_err "check missing: six failures counted, holders.json's and the five leaves figures" "stage1-check: 6 of ${FIGURES} figures failed"

# Duplicated: a selector matching two elements, a key twice in one object, a figure named twice.
cp -R "${TMP}/z1/out" "${TMP}/check-dup"
python3 -I - "${TMP}/check-dup" "${CI_BANDS}" "${TMP}/check-dup-bands.json" <<'PY'
import json, os, sys
d, bands_in, bands_out = sys.argv[1:]
h = json.load(open(os.path.join(d, "holders.json")))
h["statements"].append({"id": "mb_selective_job_rate_sum", "holders": 5})
json.dump(h, open(os.path.join(d, "holders.json"), "w"))
text = open(os.path.join(d, "stage1-compare.json")).read()
open(os.path.join(d, "stage1-compare.json"), "w").write(text.replace('"entries_compared":', '"entries_compared": 99, "entries_compared":', 1))
b = json.load(open(bands_in))
b["figures"].append(dict(b["figures"][0]))
json.dump(b, open(bands_out, "w"))
PY
run_check dup "${TMP}/check-dup-bands.json" "${TMP}/check-dup"
expect_rc "check duplicated" 1
expect_check_err "check duplicated: a selector matching two elements" "duplicated holders_selective_job_rate_sum: 2 elements of statements have id=mb_selective_job_rate_sum (holders.json statements[id=mb_selective_job_rate_sum].holders)"
expect_check_err "check duplicated: a key twice in its object" "duplicated entries_compared: entries_compared occurs more than once in the top level (stage1-compare.json entries_compared)"
expect_check_err "check duplicated: a figure the band file names twice" "duplicated leaves_parts_rebuilt: the band file names it 2 times (stamps.json stage1.leaves.parts_rebuilt)"
if [ "$(grep -c '^duplicated leaves_parts_rebuilt: ' "${CHECK_ERR}")" -eq 1 ]; then pass "check duplicated: the figure reported once, not once per copy"; else fail "check duplicated: leaves_parts_rebuilt reported $(grep -c '^duplicated leaves_parts_rebuilt: ' "${CHECK_ERR}") times"; fi

# Not finite: a NaN figure is out of band, a NaN bound refuses the band file.
cp -R "${TMP}/z1/out" "${TMP}/check-nan"
python3 -I - "${TMP}/check-nan/stamps.json" "${CI_BANDS}" "${TMP}/check-nan-bands.json" <<'PY'
import json, sys
stamps, bands_in, bands_out = sys.argv[1:]
d = json.load(open(stamps))
d["stage1"]["leaves"]["parts_rebuilt"] = float("nan")
d["stage1"]["leaves"]["parts_over_cap"] = float("inf")
json.dump(d, open(stamps, "w"))
b = json.load(open(bands_in))
b["figures"][0]["min"] = float("nan")
json.dump(b, open(bands_out, "w"))
PY
run_check nan "${CI_BANDS}" "${TMP}/check-nan"
expect_rc "check not finite" 1
expect_check_err "check not finite: NaN is not a number" "out of band leaves_parts_rebuilt = NaN: not a number, band [1, null] (stamps.json stage1.leaves.parts_rebuilt)"
expect_check_err "check not finite: Infinity is not a number" "out of band leaves_parts_over_cap = Infinity: not a number, band [0, 0] (stamps.json stage1.leaves.parts_over_cap)"
expect_check_err "check not finite: two failures counted" "stage1-check: 2 of ${FIGURES} figures failed"
run_check nan-bands "${TMP}/check-nan-bands.json" "${TMP}/z1/out"
expect_rc "check not-finite bound" 65
expect_check_err "check not-finite bound: refused, the bound named" "stage1-check: ${TMP}/check-nan-bands.json: figure leaves_parts_rebuilt: min is not a number or null"

# z15's failed routed record and z16's pruned unrouted record fail the ci bands.
run_check failed-record "${CI_BANDS}" "${TMP}/z15/out"
expect_rc "check failed record" 1
expect_check_err "check failed record: the status difference named" "out of band statements_status_differing = 1 not in [0, 0] (stage1-compare.json statements_status_differing_count)"
expect_check_err "check failed record: the failed status named" "out of band routed_records_status_unexpected = 1 not in [0, 0] (stage1-compare.json passes.routed.records_status_unexpected)"
expect_check_err "check failed record: the entry without a hash named" "out of band entries_without_hash = 1 not in [0, 0] (stage1-compare.json entries_without_hash_count)"
expect_check_err "check failed record: its routed prune unchecked" "out of band holders_routed_prune_unchecked = 1 not in [0, 0] (stamps.json stage1.holders.routed_prune_unchecked_count)"
expect_check_err "check failed record: five failures counted, with its missing segmentsPrunedByLabelIndex in the compare and the holders check" "stage1-check: 5 of ${FIGURES} figures failed"
run_check incorrect-both "${CI_BANDS}" "${TMP}/z21/out"
expect_rc "check incorrect in both passes" 0
run_check routed-status "${CI_BANDS}" "${TMP}/z22/out"
expect_rc "check routed status" 1
expect_check_err "check routed status: the status difference named" "out of band statements_status_differing = 1 not in [0, 0] (stage1-compare.json statements_status_differing_count)"
expect_check_err "check routed status: one failure counted" "stage1-check: 1 of ${FIGURES} figures failed"
run_check named-split "${CI_BANDS}" "${TMP}/z20/out"
expect_rc "check an object outside the name's postings and an absent metric" 0
# A cold count that is absent fails as missing, never as 0.
cp -R "${TMP}/z1/out" "${TMP}/check-cold"
python3 -I - "${TMP}/check-cold/stamps.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d["stage1"]["routed_cold_records"] = "failed: stage1.routed_cold is 'failed: cold.json not written', not a cold pass stamp"
json.dump(d, open(sys.argv[1], "w"))
PY
run_check cold "${CI_BANDS}" "${TMP}/check-cold"
expect_rc "check cold count missing" 1
expect_check_err "check cold count missing: named" "missing routed_cold_records: stage1.routed_cold_records is str \"failed: stage1.routed_cold is 'failed: cold.json not written', not a cold pass stamp\", not an object (stamps.json stage1.routed_cold_records.records)"
expect_check_err "check cold count missing: both figures counted" "stage1-check: 2 of ${FIGURES} figures failed"
run_check unrouted-pruned "${CI_BANDS}" "${TMP}/z16/out"
expect_rc "check unrouted pruned" 1
expect_check_err "check unrouted pruned: named" "out of band unrouted_records_pruned_by_label_index = 1 not in [0, 0] (stage1-compare.json passes.unrouted.records_pruned_by_label_index)"
run_check window "${CI_BANDS}" "${TMP}/z17/out"
expect_rc "check window mismatch" 1
if grep -qE '^out of band holders_window_mismatches = [0-9]+ not in \[0, 0\] ' "${CHECK_ERR}"; then pass "check window mismatch: named"; else fail "check window mismatch: stderr $(cat "${CHECK_ERR}")"; fi
run_check routed-prunes "${CI_BANDS}" "${TMP}/z25/out"
expect_rc "check routed prune mismatch" 1
expect_check_err "check routed prune mismatch: named" "out of band holders_routed_prune_mismatches = 1 not in [0, 0] (stamps.json stage1.holders.routed_prune_mismatches_count)"
expect_check_err "check routed prune mismatch: one failure counted" "stage1-check: 1 of ${FIGURES} figures failed"
run_check leaf-unread "${CI_BANDS}" "${TMP}/z26/out"
expect_rc "check routed leaf unread" 1
expect_check_err "check routed leaf unread: named" "out of band holders_routed_leaf_unread = 1 not in [0, 0] (stamps.json stage1.holders.routed_leaf_unread_count)"
expect_check_err "check routed leaf unread: one failure counted" "stage1-check: 1 of ${FIGURES} figures failed"
run_check leaf-unread-unbound "${CI_BANDS}" "${TMP}/z26u/out"
expect_rc "check routed leaf unread on a statement that is not route_bound" 0
# when: a figure is checked only while its condition holds; a condition that
# cannot be read fails like a figure, and a malformed one refuses the file.
python3 -I - "${TMP}/check-when" <<'PY'
import json, sys
base = {"schema": "metricsbench-stage1-bands/1", "profile": "ci"}
fig = {"file": "stamps.json", "path": "stage1.leaves.parts_rebuilt", "min": 1, "max": None, "basis": "b"}
def write(name, figures):
    json.dump({**base, "figures": figures}, open(f"{sys.argv[1]}-{name}.json", "w"))
write("holds", [{**fig, "name": "held", "when": {"file": "stamps.json", "path": "profile", "equals": "ci"}},
                {**fig, "name": "not_held", "min": 99, "when": {"file": "stamps.json", "path": "profile", "equals": "box"}}])
write("unread", [{**fig, "name": "unread", "when": {"file": "stamps.json", "path": "stage1.no_such_switch", "equals": True}}])
write("shape", [{**fig, "name": "shape", "when": {"file": "stamps.json", "path": "profile"}}])
write("scalar", [{**fig, "name": "scalar", "when": {"file": "stamps.json", "path": "profile", "equals": ["ci"]}}])
PY
run_check when-holds "${TMP}/check-when-holds.json" "${TMP}/z1/out"
expect_rc "check when" 0
if [ "$(cat "${CHECK_OUT}")" = 'ok held = 3 in [1, null] (stamps.json stage1.leaves.parts_rebuilt)
n/a not_held: profile is "ci", not "box" (stamps.json stage1.leaves.parts_rebuilt)' ]; then
  pass "check when: the held figure checked, the other reported not applicable instead of out of band"
else
  fail "check when: stdout $(cat "${CHECK_OUT}"), stderr $(cat "${CHECK_ERR}")"
fi
run_check when-unread "${TMP}/check-when-unread.json" "${TMP}/z1/out"
expect_rc "check when unread" 1
expect_check_err "check when unread: the condition's missing value named" "missing unread: when stamps.json stage1.no_such_switch: no no_such_switch in stage1 (stamps.json stage1.leaves.parts_rebuilt)"
run_check when-shape "${TMP}/check-when-shape.json" "${TMP}/z1/out"
expect_rc "check when without equals" 65
expect_check_err "check when without equals: refused" "stage1-check: ${TMP}/check-when-shape.json: figure shape: when is not an object of exactly file, path and equals"
run_check when-scalar "${TMP}/check-when-scalar.json" "${TMP}/z1/out"
expect_rc "check when equals a list" 65
expect_check_err "check when equals a list: refused" "stage1-check: ${TMP}/check-when-scalar.json: figure scalar: when.equals is not a JSON scalar"

run_check route-values "${CI_BANDS}" "${TMP}/z30/out"
expect_rc "check per-value route" 1
expect_check_err "check per-value route: the statement over its leaf GET bound named" "out of band route_values_leaf_gets_over_bound = 1 not in [0, 0] (stage1-compare.json statements_leaf_gets_over_bound_count)"
expect_check_err "check per-value route: the part beyond the ceiling named" "out of band route_values_parts_beyond_ceiling = 1 not in [0, 0] (stage1-compare.json parts_beyond_ceiling_total)"
expect_check_err "check per-value route: the unreadable record named" "out of band route_values_unreadable = 1 not in [0, 0] (stage1-compare.json routed_records_values_unreadable_count)"
expect_check_err "check per-value route: three failures counted" "stage1-check: 3 of ${FIGURES} figures failed"
cp -R "${TMP}/z1/out" "${TMP}/check-no-values"
python3 -I - "${TMP}/check-no-values/stage1-compare.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d["routed_records_without_values"] += d["routed_records_with_values"]
d["routed_records_with_values"] = 0
json.dump(d, open(sys.argv[1], "w"))
PY
run_check no-values "${CI_BANDS}" "${TMP}/check-no-values"
expect_rc "check per-value route not measured" 1
expect_check_err "check per-value route not measured: named" "out of band route_values_records = 0 not in [1, null] (stage1-compare.json routed_records_with_values)"
# Run 42495422937's figures hold every ci band; that run failed 2 of 27 figures
# against the band file before this one.
run_check run-42495422937 "${CI_BANDS}" "${TMP}/z27/out"
expect_rc "check run 42495422937" 0
if grep -qxF 'ok holders_window_mismatches = 0 in [0, 0] (stamps.json stage1.holders.window_mismatches_count)' "${CHECK_OUT}" &&
  grep -qxF 'ok holders_routed_prune_mismatches = 0 in [0, 0] (stamps.json stage1.holders.routed_prune_mismatches_count)' "${CHECK_OUT}" &&
  grep -qxF 'ok holders_routed_leaf_unread = 0 in [0, 0] (stamps.json stage1.holders.routed_leaf_unread_count)' "${CHECK_OUT}" &&
  ! grep -qF ' routed_records_pruned_by_label_index ' "${CHECK_OUT}"; then
  pass "check run 42495422937: the window, routed prune and leaf figures are 0, and no band asks the routed pass to prune"
else
  fail "check run 42495422937: stdout $(cat "${CHECK_OUT}"), stderr $(cat "${CHECK_ERR}")"
fi

# --- box-run.sh's band selection and check ----------------------------------------
BOXRUN="${HERE}/../aws/box-run.sh"
# Sourcing box-run.sh without its return guard would run the box itself
# (apt-get, the build, shutdown), so the cases below need the guard first.
if grep -qxF '[ "${BASH_SOURCE[0]}" = "$0" ] || return 0' "${BOXRUN}"; then
  pass "box-run: sourcing it defines its functions and returns before the box steps"
else
  fail "box-run: no source guard; its band cases are not run"
  BOXRUN=/dev/null
fi
# box_bands NAME PROFILE [VAR=VALUE ...]: stage1_bands PROFILE in a subshell with
# the box-run functions sourced; rc, BOX_OUT and BOX_ERR.
box_bands() {
  local name="$1" prof="$2"
  shift 2
  BOX_OUT="${TMP}/box-${name}.out"
  BOX_ERR="${TMP}/box-${name}.err"
  rc=0
  (unset MB_STAGE1 MB_STAGE1_BANDS; export "$@"; . "${BOXRUN}"; stage1_bands "${prof}") >"${BOX_OUT}" 2>"${BOX_ERR}" || rc=$?
}
box_bands off cardinality MB_STAGE1=
if [ "${rc}" -eq 0 ] && [ ! -s "${BOX_OUT}" ]; then pass "box-run: no band file without MB_STAGE1=1"; else fail "box-run off: rc ${rc}, out $(cat "${BOX_OUT}")"; fi
box_bands ci ci MB_STAGE1=1
if [ "${rc}" -eq 0 ] && [ "$(cat "${BOX_OUT}")" = "deploy/metricsbench/stage1-bands.ci.json" ]; then pass "box-run: ci reads the shipped ci bands"; else fail "box-run ci: rc ${rc}, out $(cat "${BOX_OUT}")"; fi
box_bands card cardinality MB_STAGE1=1
if [ "${rc}" -eq 1 ] && [ ! -s "${BOX_OUT}" ] &&
  grep -qxF "MB_STAGE1=1 with profile cardinality needs MB_STAGE1_BANDS, a band file pre-registered for that profile" "${BOX_ERR}"; then
  pass "box-run: cardinality without MB_STAGE1_BANDS is refused"
else
  fail "box-run cardinality: rc ${rc}, out $(cat "${BOX_OUT}"), err $(cat "${BOX_ERR}")"
fi
box_bands set ci MB_STAGE1=1 MB_STAGE1_BANDS=bands/x.json
if [ "${rc}" -eq 0 ] && [ "$(cat "${BOX_OUT}")" = "bands/x.json" ]; then pass "box-run: MB_STAGE1_BANDS wins"; else fail "box-run set: rc ${rc}, out $(cat "${BOX_OUT}")"; fi
# stage1_check runs from the source tree's root, as the box does.
for c in clean oob; do
  src="${TMP}/z1/out"
  [ "$c" = clean ] || src="${TMP}/z2/out"
  cp -R "${src}" "${TMP}/box-check-${c}"
  rc=0
  (cd "${HERE}/../../.." && . "${BOXRUN}" && stage1_check "${TMP}/box-check-${c}" deploy/metricsbench/stage1-bands.ci.json) >/dev/null 2>&1 || rc=$?
  want=0
  [ "$c" = clean ] || want=1
  if [ "${rc}" -eq "${want}" ] && [ -s "${TMP}/box-check-${c}/stage1-check.txt" ]; then
    pass "box-run: stage1_check on the ${c} run returns ${want} and keeps its output in stage1-check.txt"
  else
    fail "box-run: stage1_check on the ${c} run returned ${rc}, want ${want}"
  fi
done
if grep -qxF "out of band entries_differing = 1 not in [0, 0] (stage1-compare.json entries_differing_count)" "${TMP}/box-check-oob/stage1-check.txt"; then
  pass "box-run: stage1-check.txt carries the failure"
else
  fail "box-run: stage1-check.txt is $(cat "${TMP}/box-check-oob/stage1-check.txt")"
fi
# The measured run's band check: a failure ends the box non-zero (23) once the
# run itself passed, after uploading the outputs.
if grep -qxF '  stage1_check /mb/out-run "$run_bands" || check=$?' "${BOXRUN}" &&
  grep -qxF '    finish 23 30' "${BOXRUN}" &&
  grep -qxF 'run_bands="$(stage1_bands "$PROFILE")" || finish 21' "${BOXRUN}"; then
  pass "box-run: the measured run is checked against its bands, and a band failure or a missing band file ends the box non-zero"
else
  fail "box-run: the measured run's band check or its exits are not wired"
fi

# The band file's profile must be the run's; bad usage is 64.
run_check profile "${CI_BANDS}" "${TMP}/z/out"
expect_rc "check profile" 65
if grep -qF "the band file is for profile 'ci', the run is 'cardinality'" "${CHECK_ERR}"; then pass "check profile: the mismatch named"; else fail "check profile: stderr $(cat "${CHECK_ERR}")"; fi
rc=0
bash "${CHECK}" "${CI_BANDS}" >/dev/null 2>&1 || rc=$?
expect_rc "check usage" 64

# The captures a run ends before are stamped not taken, never null.
OUT="${TMP}/b/out"
check "b: an ingest failure leaves both captures stamped not taken" 'j("stamps.json")["object_spans"] == "not taken: the run ended before the object spans step" and j("stamps.json")["cold_ravel_diagnostic"] == "not taken: the run ended before the cold pass"'
for c in u1 u2b u3; do
  OUT="${TMP}/${c}/out"
  check "${c}: a failed folded step leaves object_spans stamped not taken" 'j("stamps.json")["object_spans"] == "not taken: the run ended before the object spans step" and j("stamps.json")["cold_ravel_diagnostic"].startswith("not applicable: no entry of class long_range")'
done

echo
echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
