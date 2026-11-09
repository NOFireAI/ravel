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
#               /metrics, stamped. The same scrape is kept as
#               <out>/ravel-metrics-at-query.txt, and Mimir's /metrics is
#               scraped beside it into <out>/mimir-metrics-at-query.txt
#   query       metricsbench_query against all four engines into
#               <out>/report.json, with the per-entry ULP gaps and oracle
#               retries in <out>/diagnostics.json. Once it has written the
#               report, Ravel's and Mimir's /metrics are scraped again into
#               <out>/ravel-metrics-after-query.txt and
#               <out>/mimir-metrics-after-query.txt. The four scrapes are
#               ADR-0927's 2026-11-04 cache-state disclosure (each engine's
#               after scrape less its before scrape). Their outcomes are
#               stamped as query_scrapes, each "ok" or why not; a failed
#               scrape never fails the run, and a failed new scrape leaves
#               no file
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
#   start       docker compose up -d --no-deps --no-recreate ravel-server:
#               the stopped container alone, since compose start would also
#               rerun the one-shot createbuckets and ravel-qualify. Then its
#               readiness URL answers 200 as in ready. Stamped as
#               ravel_restart
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
# With MB_STAGE1=1 in the environment (ADR-3694 Stage 1, issue #3968; ci and
# cardinality only, any other profile is refused with exit 65, and a
# diagnostic run is a usage error) ravel-server runs with
# --fold-key-index-leaves and without --fold-metrics-label-index from up
# onward: RAVEL_SERVER_EXTRA_FLAGS, which ravel.yml appends to the server's
# entrypoint, is set to it (it is empty otherwise, so the default command line
# is unchanged). The main report.json's provenance then carries
# comparable=false and a comparability_reason naming those flags. After
# readback-folded, and only when it ran, these steps follow, each failure
# ending the run with exit 70 naming it. Every ravel-server recreate below is
# compose up -d --no-deps --force-recreate ravel-server with
# RAVEL_SERVER_EXTRA_FLAGS set as named, then its readiness URL answering 200;
# each passes --disable-fold, so from stage1-fold onward only stage1-fold and
# stage1-leaves change the snapshot (stamped as stage1.background_fold):
#
#   stage1-fold  ravel-server recreated with --disable-fold
#               --fold-key-index-leaves (stamped as stage1.fold_restart), then
#               ravel-cli catalog fold --signal metrics --json on
#               ravel-qualify, with no leaf flag and no --writers-stopped, into
#               <out>/stage1-fold.json; stamped as stage1.fold with its wall time
#   stage1-unrouted  ravel-server recreated with the same flags (stamped as
#               stage1.unrouted_restart). Then ravel-cli catalog inspect
#               --signal metrics into <out>/stage1-unrouted.inspect.txt,
#               stamped as stage1.unrouted_catalog: the HEAD's watermark_hour,
#               its part count and the parts carrying a label_value key-index
#               leaf ref, with the source named; the step is refused when that
#               text cannot be read, the stamp cannot be written or its
#               leaf part count read back as a whole number, or any part
#               carries such a leaf. Then
#               metricsbench_query against Prometheus and Ravel with the full
#               corpus and --passes/--reps, --pass-label folded-unrouted and a
#               fold_state naming those figures and the server flags, into
#               <out>/stage1-unrouted/ (report.json, diagnostics.json,
#               provenance.json), then the cold pass's selection (see
#               cold_ravel_diagnostic) the same way into
#               <out>/stage1-unrouted/cold.json (a cold failure is stamped,
#               never fatal), its records with a ravel_cost counted against
#               the selected entries x repetitions as
#               stage1.unrouted_cold_records (0 of 0 when no entry applies)
#   stage1-leaves  ravel-cli catalog fold --rebuild-key-index --signal metrics
#               --metrics-label-index --json into <out>/stage1-leaves.json;
#               stamped as stage1.leaves: the report's leaf, part and byte
#               counts, wall_ms, and under requests every request count the
#               report gives, each by its own name (data object ranged GETs,
#               leaf prefix GETs, erasure LISTs and GETs, and the per-attempt
#               part object GETs, leaf PUTs and HEAD CAS PUTs summed); the
#               report counts no HEAD GET, and the stamp says so
#   stage1-restart  ravel-server recreated with --disable-fold
#               --fold-key-index-leaves --fold-metrics-label-index
#   stage1-routed  the inspect again, into <out>/stage1-routed.inspect.txt and
#               stage1.routed_catalog, refused when fewer parts carry a
#               label_value leaf than stage1-leaves rebuilt (parts_rebuilt);
#               then the passes as stage1-unrouted, --pass-label
#               folded-routed, into <out>/stage1-routed/. Then
#               <out>/stage1-compare.json: per entry, the steady passes'
#               result_hash sets compared (an entry whose two passes carry
#               the same set of two or more hashes counted as multi-hash, not
#               as differing) and status sets compared (ADR-3694
#               row 9: the entries whose set of statuses differs between the
#               passes), and per pass the records with a status other than ok
#               or incorrect, the ok or incorrect records with a null
#               result_hash, the records with no segmentsPrunedByLabelIndex
#               figure (counted apart, never as 0) and with one above 0;
#               stamped as stage1.compare. A difference
#               is recorded, not fatal: stage1-check.sh's band file is the
#               verdict
#   stage1-holders  ravel-cli catalog list --json (the L0 and L1 data objects
#               the folded snapshot resolves) into <out>/stage1-objects.jsonl;
#               the bucket's L0 and L1 data objects downloaded by aws s3 sync
#               on the createbuckets service into <out>/stage1-objects/;
#               metricsbench_holders over the listed ones into
#               <out>/holders.json, the download deleted. Stamped as
#               stage1.holders: per statement segments_in_window,
#               named_in_window, token_only_fetched,
#               unrouted_fetched_expected, holders (a series-level index's
#               bound), value_holders (the label-value router's bound),
#               expected_routed_prunes, the routed records'
#               segmentsPrunedByLabelIndex values and route_bound; the method;
#               the statements an unrouted record of which has a
#               segmentsFetched other than unrouted_fetched_expected, and the
#               route-bound statements a routed record of which has a
#               segmentsPrunedByLabelIndex other than expected_routed_prunes
#               (each with those that could not be checked); the routed
#               records of route-bound statements whose keyIndex phase shows
#               no leaf read; and the statements that are not route-bound
#
# Four arms (issue #3969) add steps, each only when its switch is on;
# stamps.json says which under stage1.arms, and an arm that is off stamps
# each of its keys "skipped: arm off (...)". MB_CONTROL_IMAGE (with
# MB_CONTROL_SOURCE_COMMIT) turns on the control arm, which runs between
# stage1-unrouted and stage1-leaves; MB_STAGE1_ARMS, a comma list of
# extra-hour, l1 and erasure, turns on the arms that change the tenant, which
# run after stage1-holders in that order:
#
#   stage1-control  (ADR-3694 row 12) refused with exit 65 unless
#               MB_CONTROL_IMAGE carries an @sha256: digest and
#               MB_CONTROL_SOURCE_COMMIT is 40 hex digits. ravel-server
#               recreated from the control image with the unrouted flags
#               (stage1.control_restart), docker image inspect required to
#               report that digest, both images' digests and source commits
#               stamped as stage1.control_image, then the inspect and the
#               passes as stage1-unrouted, --pass-label folded-control, into
#               <out>/stage1-control/
#   stage1-control-restore  ravel-server recreated from the main image with
#               the unrouted flags. Then <out>/stage1-control-compare.json:
#               per statement the median latency_ms ratio (unrouted over
#               control) and the median GETs and wire bytes, in total and per
#               phase, of each pass, and the statements whose unrouted
#               median latency exceeds the control's by more than 5% and by
#               more than either pass's spread; stamped as
#               stage1.control_compare.
#               With the extra-hour arm, the metrics catalog HEAD is saved
#               with aws s3 cp on createbuckets before stage1-leaves (the
#               leafless HEAD) and after it (the routed HEAD)
#   stage1-extra-hour-ingest  (row 7) once the wall clock passes the routed
#               snapshot's watermark hour (METRICSBENCH_SLEEP waits),
#               metricsbench_ingest writes the profile's next hour, Ravel
#               only, into <out>/stage1-extra-hour/
#   stage1-extra-hour-fold-on  ravel-server stopped, the routed HEAD put
#               back and read back (its sha256 must match), then catalog
#               fold --writers-stopped --key-index-leaves
#               --metrics-label-index timed (stage1.extra_hour_fold_on) and
#               its HEAD saved
#   stage1-extra-hour-fold-off  the leafless HEAD put back and read back,
#               then catalog fold --writers-stopped with no leaf flag timed
#               (stage1.extra_hour_fold_off), refused unless it reports
#               key_index_switch_off_over_leaves false
#   stage1-extra-hour  the leaf-on fold's HEAD put back and read back,
#               inspect and listing into
#               <out>/stage1-extra-hour.inspect.txt and .objects.jsonl;
#               stamped as stage1.extra_hour: both wall times and their
#               ratio, the leaf bytes over the bytes of the listed objects
#               the tail parts cover, the tail parts with no label_value
#               leaf or an uncovered entry; ravel-server recreated with the
#               routed flags
#   stage1-l1-compact  (row 6) once the newest ingest hour is sealed,
#               ravel-server stopped and ravel-cli maintain compact-tenant
#               --signal metrics --max-flush-lifetime 0s into
#               <out>/stage1-l1-compact.txt (stage1.l1_compact)
#   stage1-l1-fold  catalog fold --writers-stopped --key-index-leaves
#               --metrics-label-index (stage1.l1_fold)
#   stage1-l1   ravel-server recreated with the routed flags, the listing's
#               L0 and L1 objects stamped as stage1.l1_objects, the passes
#               as stage1-routed with --pass-label folded-l1 (steady only),
#               and <out>/stage1-l1-compare.json comparing their hashes with
#               stage1-routed's and counting the statuses other than ok
#               and incorrect
#   stage1-erasure-reference  (row 8) the corpus with
#               instance!="metricsbench-instance-0" added to every selector,
#               into <out>/stage1-erasure-reference.corpus.json, run routed;
#               <out>/stage1-erasure-subject.json names the statements whose
#               answer the subject changes
#   stage1-erasure-submit  ravel-cli erase submit --signal metrics
#               --matcher instance=metricsbench-instance-0
#   stage1-erasure-pending  the corpus as written, run routed; then
#               <out>/stage1-erasure-compare.json compares it with the
#               reference and counts label-index prunes
#   stage1-erasure-rewrite  METRICSBENCH_SLEEP until the submit's ingest
#               hour seals (its end + max_flush_lifetime 1 h + clock skew
#               5 min, stage1.erasure_seal_wait); then a one-off ravel-server
#               --mode maintain container (stage1.erasure_maintain) with a
#               throwaway record key from mktemp outside the out dir runs
#               until erase status says COMPLETED
#               (METRICSBENCH_ERASURE_TRIES x METRICSBENCH_ERASURE_INTERVAL),
#               then is removed, and the key with it
#   stage1-erasure-fold  ravel-server stopped, catalog fold --writers-stopped
#               --key-index-leaves --metrics-label-index
#   stage1-erasure  the inspect into <out>/stage1-erasure.inspect.txt;
#               stage1.erasure_leaves diffs it with the reference pass's
#               inspect: the parts rewritten (key and blake3 gone) and the
#               leaves of those parts the snapshot still references;
#               stage1.erasure_sweep says the physical sweep was not checked
#
# With or without MB_STAGE1, the query step's ravel_cost records carry
# pass_label unfolded while ravel-server has published no fold gauge, else
# background-folded, and fold_state names the gauge stamped as
# ravel_catalog_fold. When the /metrics scrape before the query step fails,
# the records carry pass_label unknown and the fold state, like the
# ravel_catalog_fold stamp, says unknown with the scrape error.
# stage1-check.sh BANDS OUT-DIR checks a run's figures against a band file.
#
# With --diagnostic-prometheus-max-samples N the run is the falsification run
# pre-registered on #2821 instead: steps override (a compose override in the
# out-dir setting Prometheus' --query.max-samples=N), up (Prometheus alone),
# ready, sampler, ingest (Prometheus only), falsification (mb_long_range_7d_avg
# once as an instant query at the newest sample, into
# <out>/falsification.json) and memory. It writes no report.json, and
# stamps.json says diagnostic: true. --ravel-image and --ravel-source-commit
# are optional there, since no Ravel is started; objects does not run
# (ravel_objects is stamped not applicable), no scrape is taken around a query
# step (query_scrapes is stamped not applicable) and --ravel-objects-band is a
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
# where fetch-s3s-fs keeps the verified binary between runs),
# METRICSBENCH_CORPUS (default benchmarks/metrics/metrics.corpus.json; the
# path used is stamped as corpus). MB_STAGE1 (1, or 0 or unset for off) adds
# the Stage 1 steps above; MB_STAGE1_ARMS, MB_CONTROL_IMAGE and
# MB_CONTROL_SOURCE_COMMIT add its arms. METRICSBENCH_SLEEP (default sleep)
# waits for an hour to pass or seal; METRICSBENCH_ERASURE_TRIES and
# METRICSBENCH_ERASURE_INTERVAL (default 120 tries 15 s apart) bound the
# erasure rewrite.
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
CORPUS="${METRICSBENCH_CORPUS:-${repo_root}/benchmarks/metrics/metrics.corpus.json}"

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

STAGE1="${MB_STAGE1:-}"
# ravel-server's extra flags in each Stage 1 phase, through ravel.yml's
# RAVEL_SERVER_EXTRA_FLAGS: from up through readback-folded the background
# fold runs; from stage1-fold onward it is off, so only the run's own folds
# change the snapshot the passes read.
STAGE1_UP_FLAGS="--fold-key-index-leaves"
STAGE1_UNROUTED_FLAGS="--disable-fold --fold-key-index-leaves"
STAGE1_ROUTED_FLAGS="--disable-fold --fold-key-index-leaves --fold-metrics-label-index"
# Every stage1.* stamp a step writes: null until it runs, "skipped: why" when
# the steps do not run.
STAGE1_KEYS="background_fold fold_restart fold unrouted_restart unrouted_catalog unrouted unrouted_cold unrouted_cold_records leaves restart routed_catalog routed routed_cold routed_cold_records compare holders"
# Each arm's stage1.* stamps: null when the arm is on, "skipped: arm off"
# when it is not.
STAGE1_ARMS="control extra_hour l1 erasure"
STAGE1_CONTROL_KEYS="control_restart control_image control_catalog control control_cold control_cold_records control_restore control_compare"
STAGE1_EXTRA_HOUR_KEYS="extra_hour_ingest extra_hour_fold_off extra_hour_fold_on extra_hour extra_hour_restart"
STAGE1_L1_KEYS="l1_compact l1_fold l1_restart l1_objects l1_catalog l1 l1_compare l1_not_measured"
STAGE1_ERASURE_KEYS="erasure_corpus erasure_reference_catalog erasure_reference erasure_subject erasure_submit erasure_pending_catalog erasure_pending erasure_compare erasure_seal_wait erasure_maintain erasure_rewrite erasure_fold erasure_catalog erasure_leaves erasure_sweep"
# The erasure arm's subject: one value of the workload's scaling label.
STAGE1_ERASURE_LABEL="instance"
STAGE1_ERASURE_VALUE="metricsbench-instance-0"
# ravel.yml's --max-ingest-lag, which the one-off maintain process repeats.
RAVEL_MAX_INGEST_LAG="45d"
# The one-off maintain process's flags; its record key file is mounted at
# /maintain.key.
STAGE1_MAINTAIN_FLAGS="--mode maintain --store s3 --tenant-hash-unkeyed --shards ${RAVEL_SHARDS} --max-ingest-lag ${RAVEL_MAX_INGEST_LAG} --maintain-tenant ${RAVEL_TENANT} --maintain-key-file /maintain.key --maintain-interval-secs 10 --disable-fold"
# The seal margin the rewrite waits out for the hour open at the erasure's
# acknowledgement, in seconds: DEFAULT_MAX_FLUSH_LIFETIME_NS (1 h) and
# DEFAULT_CLOCK_SKEW_ALLOWANCE_NS (300 s) in crates/ravel-maintain/src/config.rs,
# which the maintain process runs with (no --gc-max-flush-lifetime above).
ERASURE_MAX_FLUSH_LIFETIME_S=3600
ERASURE_CLOCK_SKEW_S=300
# The maintain record key file while the erasure rewrite runs, removed on exit.
STAGE1_KEY_FILE=""
ERASURE_TRIES="${METRICSBENCH_ERASURE_TRIES:-120}"
ERASURE_INTERVAL="${METRICSBENCH_ERASURE_INTERVAL:-15}"
SLEEP="${METRICSBENCH_SLEEP:-sleep}"

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

json_bool() { if [ "$1" -eq 1 ]; then echo true; else echo false; fi; }

stage1_arm_keys() {
  case "$1" in
    control) echo "$STAGE1_CONTROL_KEYS" ;;
    extra_hour) echo "$STAGE1_EXTRA_HOUR_KEYS" ;;
    l1) echo "$STAGE1_L1_KEYS" ;;
    erasure) echo "$STAGE1_ERASURE_KEYS" ;;
  esac
}

stage1_arm_on() {
  local v="arm_$1"
  [ "${!v}" -eq 1 ]
}

stage1_arm_switch() {
  case "$1" in
    control) echo "MB_CONTROL_IMAGE is not set" ;;
    *) echo "MB_STAGE1_ARMS does not name ${1//_/-}" ;;
  esac
}

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

def whole(v):
    return isinstance(v, int) and not isinstance(v, bool) and v >= 0

def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2

def cost_rows(path):
    rows = load(path)["ravel_cost"]
    if not isinstance(rows, list):
        raise TypeError("ravel_cost is not a list")
    return rows

def record_cost(rec):
    # {phase: [s3GetRequests, s3GetWireBytes]} summed over a record's
    # phases; None when a phase lacks either figure.
    phases = rec.get("phases")
    if not isinstance(phases, list):
        return None
    out = {}
    for p in phases:
        if not isinstance(p, dict) or not isinstance(p.get("phase"), str) or \
                not whole(p.get("s3GetRequests")) or not whole(p.get("s3GetWireBytes")):
            return None
        cur = out.setdefault(p["phase"], [0, 0])
        cur[0] += p["s3GetRequests"]
        cur[1] += p["s3GetWireBytes"]
    return out

def hash_sets(rows):
    by = {}
    for r in rows:
        by.setdefault(r.get("entry"), set())
        if r.get("result_hash"):
            by[r["entry"]].add(r["result_hash"])
    return by

def inspect_parts(path):
    # `catalog inspect` text: per part its hour range, key, blake3 and size,
    # and its key-index leaf refs.
    parts = []
    with open(path) as f:
        for l in f.read().splitlines():
            m = re.match(r"^  part\[[0-9]+\] range=([0-9]+)\.\.=([0-9]+) key=(\S+) blake3=(\S+) size=([0-9]+)", l)
            if m:
                parts.append({"lo": int(m.group(1)), "hi": int(m.group(2)), "key": m.group(3),
                              "blake3": m.group(4), "size": int(m.group(5)), "leaves": []})
                continue
            m = re.match(r"^      field=(\S+) slice=\S+ entry_count=([0-9]+) uncovered_count=([0-9]+) key=(\S+)$", l)
            if m and parts:
                parts[-1]["leaves"].append({"field": m.group(1), "entry_count": int(m.group(2)),
                                            "uncovered": int(m.group(3)), "key": m.group(4)})
    return parts

def listed_objects(path):
    rows = []
    with open(path) as f:
        for l in f:
            if l.startswith("{"):
                rows.append(json.loads(l))
    return rows

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
elif op == "object-spans":
    # `catalog list --json` stdout (one JSON document per line and per
    # segment; the store header goes to stderr), `catalog inspect` text,
    # ravel-objects.json, the output path, the tenant, then the two commands
    # as run. Exit 0 and the stamp as JSON on stdout, or 4 and why not on
    # stdout, writing nothing. SPANS_SEGMENT_FIELDS is the key list ravel-cli's
    # catalog_list_json_line test pins.
    SPANS_SEGMENT_FIELDS = ("level", "object_size", "ingest_hour_bucket", "min_event_ts_ns", "max_event_ts_ns")
    list_path, inspect_path, objects_path, out_path, tenant, list_cmd, inspect_cmd = args
    try:
        with open(list_path) as f:
            lines = [l for l in f.read().splitlines() if l.strip()]
    except OSError as e:
        print(f"the catalog listing could not be read ({type(e).__name__})")
        sys.exit(4)
    segments = []
    for n, l in enumerate(lines, 1):
        try:
            seg = json.loads(l)
        except ValueError:
            print(f"line {n} of the catalog listing is not a JSON document")
            sys.exit(4)
        if not isinstance(seg, dict) or any(k not in seg for k in SPANS_SEGMENT_FIELDS):
            print(f"line {n} of the catalog listing lacks one of {', '.join(SPANS_SEGMENT_FIELDS)}")
            sys.exit(4)
        segments.append(seg)
    if not segments:
        print("the catalog listing printed no segment line")
        sys.exit(4)
    try:
        with open(inspect_path) as f:
            m = re.search(r"^parts: ([0-9]+)$", f.read(), re.M)
    except OSError:
        m = None
    if not m:
        print("no snapshot HEAD: catalog inspect printed no parts line, so the fold published none")
        sys.exit(4)
    parts = int(m.group(1))
    if parts < 1:
        print("the snapshot HEAD covers no part")
        sys.exit(4)
    NS_H = 3600 * 10**9
    # Lower bound inclusive, upper bound exclusive, except the last two:
    # "6 to 24 h" includes 24 h and "over 24 h" is strictly above it.
    edges = [("under 1 h", lambda s: s < NS_H), ("1 to 2 h", lambda s: NS_H <= s < 2 * NS_H),
             ("2 to 6 h", lambda s: 2 * NS_H <= s < 6 * NS_H), ("6 to 24 h", lambda s: 6 * NS_H <= s <= 24 * NS_H),
             ("over 24 h", lambda s: s > 24 * NS_H)]
    hist = [{"bucket": name, "objects": 0, "bytes": 0} for name, _ in edges]
    hours = set()
    total_bytes = 0
    for seg in segments:
        try:
            span = seg["max_event_ts_ns"] - seg["min_event_ts_ns"]
            size, hour, level = seg["object_size"], seg["ingest_hour_bucket"], seg["level"]
            assert span >= 0 and isinstance(size, int) and level in ("L0", "L1")
        except (KeyError, TypeError, AssertionError):
            print(f"a listed segment has no usable event bounds, size, ingest hour or level: {seg!r}")
            sys.exit(4)
        hits = [i for i, (_, test) in enumerate(edges) if test(span)]
        assert len(hits) == 1
        hist[hits[0]]["objects"] += 1
        hist[hits[0]]["bytes"] += size
        hours.add(hour)
        total_bytes += size
    doc = {"tenant": tenant, "signal": "metrics",
           "inspector": {"entries": list_cmd, "snapshot_parts": inspect_cmd},
           "object_count": len(segments), "object_bytes": total_bytes,
           "distinct_ingest_hours": len(hours), "snapshot_parts": parts,
           "span_unit": "hours of event time (max event ts minus min event ts) per data object",
           "histogram": hist}
    try:
        data_count = load(objects_path)["data"]["count"]
    except (OSError, ValueError, KeyError, TypeError):
        data_count = None
    doc["ravel_objects_data_count"] = data_count
    # null, not false, when there is no count to compare against.
    doc["matches_ravel_objects"] = None if data_count is None else data_count == len(segments)
    dump(out_path, doc)
    print(json.dumps({"status": "ok", "file": "ravel-object-spans.json", "inspector": doc["inspector"],
                      "object_count": len(segments), "matches_ravel_objects": doc["matches_ravel_objects"]}))
elif op == "cold-select":
    # The corpus, the profile, a directory: one single-entry corpus file per
    # selected entry (class long_range, plus the named full-range entry,
    # declared for the profile), the ids one per line. Exit 3 when none is
    # selected; exit 4 and why on stdout when the corpus cannot be read or a
    # file cannot be written.
    corpus_path, prof, dest, extra_id = args
    try:
        corpus = load(corpus_path)
        entries = corpus["entries"]
        assert isinstance(entries, list)
    except (OSError, ValueError, KeyError, TypeError, AssertionError) as e:
        print(f"the corpus {corpus_path} could not be loaded ({type(e).__name__})")
        sys.exit(4)
    ids = []
    try:
        for e in entries:
            if e.get("class") != "long_range" and e.get("id") != extra_id:
                continue
            if "profiles" in e and prof not in e["profiles"]:
                continue
            dump(os.path.join(dest, f"corpus-{e['id']}.json"), {**corpus, "entries": [e]})
            ids.append(e["id"])
    except (OSError, KeyError, TypeError, AttributeError) as e:
        print(f"the single-entry corpus files could not be written from {corpus_path} ({type(e).__name__})")
        sys.exit(4)
    if not ids:
        sys.exit(3)
    print("\n".join(ids))
elif op == "cold-record":
    # runs file, entry, repetition, the query's exit code, its diagnostics
    # file: append the one run's record.
    runs_path, entry, rep, code, diag_path = args
    runs = load(runs_path) if os.path.exists(runs_path) else []
    rec = {"entry": entry, "repetition": int(rep), "status": None, "latency_ms": None, "ravel_cost": None}
    why = None
    if not code.isdigit():
        why = f"failed: {code}"
    elif code != "0":
        why = f"failed: metricsbench_query exited {code}"
    else:
        try:
            cost = [c for c in load(diag_path)["ravel_cost"] if c.get("entry") == entry]
        except (OSError, ValueError, KeyError, TypeError):
            cost = []
            why = "failed: the diagnostics file carries no ravel_cost"
        if why is None and len(cost) != 1:
            why = f"failed: {len(cost)} ravel_cost records for the entry, want exactly 1"
        elif why is None:
            rec["ravel_cost"] = cost[0]
            rec["status"] = cost[0].get("status")
            rec["latency_ms"] = cost[0].get("latency_ms")
    if why:
        rec["status"] = why
    runs.append(rec)
    dump(runs_path, runs)
elif op == "cold-final":
    # Every (entry, repetition) has exactly one record: a run whose
    # cold-record never landed gets a failed one here, naming it. Exit 4 and
    # why on stdout for a duplicate or unexpected record, writing nothing.
    runs_path, out_path, prof, regime, reps, corpus_path, *ids = args
    reps = int(reps)
    try:
        recorded = load(runs_path) if os.path.exists(runs_path) else []
        assert isinstance(recorded, list)
    except (OSError, ValueError, AssertionError):
        recorded = []
    want = [(i, r) for i in ids for r in range(1, reps + 1)]
    by_run = {}
    for rec in recorded:
        k = (rec.get("entry"), rec.get("repetition")) if isinstance(rec, dict) else None
        if k not in want or k in by_run:
            print(f"runs.json holds a duplicate or unexpected record: {rec!r}")
            sys.exit(4)
        by_run[k] = rec
    missing = [{"entry": i, "repetition": r} for i, r in want if (i, r) not in by_run]
    runs = [by_run.get((i, r)) or {"entry": i, "repetition": r, "status": "failed: no record was written for this run",
                                   "latency_ms": None, "ravel_cost": None} for i, r in want]
    if len(runs) != len(ids) * reps:
        print(f"{len(runs)} run records, want {len(ids)} entries x {reps} repetitions")
        sys.exit(4)
    doc = {"regime": regime, "comparable": False,
           "note": "diagnostic, not comparable: Ravel only, one restart per repetition, never in report.json's timed denominators; each repetition also queries the Prometheus oracle that metricsbench_query requires, and only Ravel's cost is recorded here; the cross-engine cold and warm procedure is issue #3794 (M9)",
           "profile": prof, "corpus": corpus_path, "repetitions_per_entry": reps, "entries": ids, "runs": runs}
    dump(out_path, doc)
    bad = [r for r in runs if r["ravel_cost"] is None]
    print(json.dumps({"regime": regime, "comparable": False, "file": os.path.basename(out_path),
                      "corpus": corpus_path, "entries": ids, "repetitions_per_entry": reps, "runs": len(runs),
                      "failed_runs": len(bad), "missing_runs": missing}))
elif op == "cold-count":
    # The stamps file and a cold pass's stamp key: how many cold records with
    # a ravel_cost the pass produced against the entries x repetitions it
    # selected, or 0 of 0 when the stamp says no entry applies. Exit 3 and why
    # on stdout when the stamp is neither, so the count is absent rather
    # than 0.
    stamps_path, key = args
    cur = load(stamps_path)
    for k in key.split("."):
        cur = cur.get(k) if isinstance(cur, dict) else None
    if isinstance(cur, str) and cur.startswith("not applicable:"):
        print(json.dumps({"applicable": False, "expected_records": 0, "records": 0, "records_short": 0,
                          "basis": f"{key} is {cur!r}"}))
        sys.exit(0)
    try:
        assert isinstance(cur, dict)
        entries, reps, runs, bad = cur["entries"], cur["repetitions_per_entry"], cur["runs"], cur["failed_runs"]
        assert isinstance(entries, list) and all(isinstance(v, int) and not isinstance(v, bool) for v in (reps, runs, bad))
    except (AssertionError, KeyError):
        print(f"{key} is {json.dumps(cur)[:200]}, not a cold pass stamp")
        sys.exit(3)
    expected = len(entries) * reps
    print(json.dumps({"applicable": True, "expected_records": expected, "records": runs - bad,
                      "records_short": expected - (runs - bad),
                      "basis": f"{key}: records with a ravel_cost (runs - failed_runs) against entries x repetitions_per_entry"}))
elif op in ("stage1-fold", "stage1-leaves"):
    # ravel-cli catalog fold --json's report, the step's wall time in ms:
    # print the stamp. Exit 3 and why on stdout when the report is not JSON
    # or lacks a field the stamp reads.
    report_path, wall_ms = args
    try:
        rep = load(report_path)
        assert isinstance(rep, dict)
    except (OSError, ValueError, AssertionError) as e:
        print(f"{os.path.basename(report_path)} is not a JSON report ({type(e).__name__})")
        sys.exit(3)
    if op == "stage1-fold":
        want = ["watermark_hour", "previous_watermark_hour", "no_op", "entry_count", "buckets_folded",
                "list_requests", "get_requests", "put_requests", "key_index_leaves_written",
                "key_index_leaf_bytes_written", "key_index_switch_off_over_leaves"]
    else:
        want = ["leaves_written", "leaf_bytes_written", "ranged_gets", "leaf_prefix_gets",
                "erasure_get_requests", "erasure_list_requests", "leaf_prefix_bytes_read",
                "section_bytes_read", "parts_unindexed", "parts_rebuilt", "parts_deferred",
                "parts_over_cap", "parts_erasure_gated", "uncovered_entries", "attempt_count"]
    missing = [k for k in want if k not in rep]
    if missing:
        print(f"{os.path.basename(report_path)} lacks " + ", ".join(missing))
        sys.exit(3)
    stamp = {"file": os.path.basename(report_path), "wall_ms": int(wall_ms)}
    stamp.update({k: rep[k] for k in want})
    if op == "stage1-leaves":
        # Every request count the rebuild report carries, each under its own
        # name: the top-level counts and the per-batch ones summed over
        # attempts. The report counts no GET of the HEAD itself.
        def whole(v):
            return isinstance(v, int) and not isinstance(v, bool) and v >= 0
        attempts = rep.get("attempts")
        if not isinstance(attempts, list) or not all(isinstance(a, dict) for a in attempts):
            print(f"{os.path.basename(report_path)} has no attempts list")
            sys.exit(3)
        per_batch = {"part_object_gets": "part_gets", "leaf_puts": "leaf_puts", "head_cas_puts": "head_cas_tries"}
        top = {"data_object_ranged_gets": "ranged_gets", "leaf_prefix_gets": "leaf_prefix_gets",
               "erasure_list_requests": "erasure_list_requests", "erasure_get_requests": "erasure_get_requests"}
        if not all(whole(rep[k]) for k in top.values()) or \
                not all(whole(a.get(k)) for a in attempts for k in per_batch.values()):
            print("a request count in the rebuild report is not a whole number")
            sys.exit(3)
        requests = {name: rep[k] for name, k in top.items()}
        requests.update({name: sum(a[k] for a in attempts) for name, k in per_batch.items()})
        requests["not_reported"] = "GETs of the snapshot HEAD (the rebuild report does not count them)"
        stamp["requests"] = requests
    print(json.dumps(stamp))
elif op == "stage1-compare":
    # The two passes' diagnostics files and the output path. Per entry, the
    # set of result_hash values each pass's records carry; equal when both are
    # present and the same single hash, multi-hash when both are the same set
    # of two or more (the passes agree, but a pass did not repeat its own
    # answer), differing when the sets differ. Prints the summary; exit 3 and
    # why on stdout when a diagnostics file cannot be read.
    unrouted_path, routed_path, out_path = args
    def records(path):
        doc = load(path)
        rows = doc["ravel_cost"]
        if not isinstance(rows, list):
            raise TypeError("ravel_cost is not a list")
        return rows
    try:
        unrouted, routed = records(unrouted_path), records(routed_path)
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"the stage1 diagnostics could not be read ({type(e).__name__}: {e})")
        sys.exit(3)
    def hashes(rows):
        by = {}
        for r in rows:
            by.setdefault(r.get("entry"), set())
            if r.get("result_hash"):
                by[r["entry"]].add(r["result_hash"])
        return by
    u, r = hashes(unrouted), hashes(routed)
    equal, multi, differing, without = [], [], [], []
    for entry in sorted(set(u) | set(r), key=str):
        a, b = u.get(entry, set()), r.get(entry, set())
        if not a or not b:
            without.append(entry)
        elif a == b and len(a) == 1:
            equal.append(entry)
        elif a == b:
            multi.append({"entry": entry, "hashes": sorted(a)})
        else:
            differing.append({"entry": entry, "unrouted": sorted(a), "routed": sorted(b)})
    def pruned(rec):
        # None when the record carries no whole-number figure: counted apart,
        # never as 0.
        v = rec.get("segmentsPrunedByLabelIndex")
        return v if isinstance(v, int) and not isinstance(v, bool) else None
    # ADR-3694 row 9: the oracle's verdict on a statement is the set of
    # statuses its records carry, and routing must not change it. ok and
    # incorrect are the two answered statuses (a statement known to differ
    # from Prometheus stays incorrect in both passes); any other status is a
    # record that did not answer.
    ANSWERED = ("ok", "incorrect")
    def statuses(rows):
        by = {}
        for x in rows:
            by.setdefault(x.get("entry"), set()).add(str(x.get("status")))
        return by
    us, rs = statuses(unrouted), statuses(routed)
    status_differing = [{"entry": e, "unrouted": sorted(us.get(e, set())), "routed": sorted(rs.get(e, set()))}
                        for e in sorted(set(us) | set(rs), key=str) if us.get(e) != rs.get(e)]
    def tally(rows):
        figures = [pruned(x) for x in rows]
        answered = [x for x in rows if x.get("status") in ANSWERED]
        return {"records": len(rows),
                "records_answered": len(answered),
                "records_answered_null_hash": sum(1 for x in answered if not x.get("result_hash")),
                "records_status_unexpected": len(rows) - len(answered),
                "records_label_index_null": sum(1 for v in figures if v is None),
                "records_pruned_by_label_index": sum(1 for v in figures if v is not None and v > 0),
                "segments_pruned_by_label_index": sum(v for v in figures if v is not None)}
    # ADR-3694's per-value routing amendment: each distinct positive equality
    # value is its own routing pass, so a statement with n values may issue
    # up to n passes' leaf GETs. Per routed record, the passes' figures
    # summed; covering parts is the most any pass covered (every pass routes
    # the same snapshot).
    VALUE_FIELDS = ("leafGetRequests", "leafCacheHits", "partsCovered", "partsBeyondCeiling", "segmentsPrunedAlone")
    route_values, values_null, values_unreadable, over_bound = [], 0, [], set()
    for x in routed:
        vals = x.get("labelIndexValues")
        if vals is None:
            values_null += 1
            continue
        if not isinstance(vals, list) or not all(
                isinstance(v, dict) and isinstance(v.get("labels"), list)
                and all(isinstance(v.get(k), int) and not isinstance(v.get(k), bool) and v[k] >= 0 for k in VALUE_FIELDS)
                for v in vals):
            values_unreadable.append({"entry": x.get("entry"), "rep": x.get("rep")})
            continue
        n = len(vals)
        covering = max((v["partsCovered"] for v in vals), default=0)
        leaf_gets = sum(v["leafGetRequests"] for v in vals)
        named = [v for v in vals if "__name__" in v["labels"]]
        row = {"entry": x.get("entry"), "rep": x.get("rep"), "n": n, "leaf_gets": leaf_gets,
               "leaf_cache_hits": sum(v["leafCacheHits"] for v in vals),
               "covering_parts": covering, "leaf_gets_bound": (1 + n) * covering,
               "parts_beyond_ceiling": sum(v["partsBeyondCeiling"] for v in vals),
               "name_pruned_alone": any(v["segmentsPrunedAlone"] > 0 for v in named) if named else None}
        if leaf_gets > row["leaf_gets_bound"]:
            over_bound.add(x.get("entry"))
        route_values.append(row)
    doc = {"schema": "metricsbench-stage1-compare/5",
           "unrouted": os.path.basename(os.path.dirname(unrouted_path)) + "/" + os.path.basename(unrouted_path),
           "routed": os.path.basename(os.path.dirname(routed_path)) + "/" + os.path.basename(routed_path),
           "entries_compared": len(equal) + len(multi) + len(differing), "entries_equal": len(equal),
           "entries_multi_hash_count": len(multi), "entries_multi_hash": multi,
           "entries_differing_count": len(differing), "entries_differing": differing,
           "entries_without_hash_count": len(without), "entries_without_hash": without,
           "status_check": "per entry, the set of statuses its stage1-unrouted records carry == the set its stage1-routed records carry (ADR-3694 row 9); records_status_unexpected counts records whose status is neither ok nor incorrect; records_answered_null_hash counts ok or incorrect records with no result_hash",
           "statements_status_differing_count": len(status_differing),
           "statements_status_differing": status_differing,
           "passes": {"unrouted": tally(unrouted), "routed": tally(routed)},
           "route_values_check": "per stage1-routed record whose labelIndexValues is a list (one element per routing pass, ADR-3694's per-value routing amendment): n distinct values, leaf_gets the passes' leafGetRequests summed, leaf_cache_hits their leafCacheHits summed, covering_parts the most partsCovered of any pass, leaf_gets_bound (1 + n) x covering_parts, parts_beyond_ceiling the passes' partsBeyondCeiling summed, and name_pruned_alone whether the __name__ pass pruned a segment no other value of a plan pruned (null when no pass carries __name__); a record with a null labelIndexValues (the route did not run) is counted in routed_records_without_values, one whose passes lack a whole-number figure in routed_records_values_unreadable",
           "routed_values": route_values,
           "routed_records_with_values": len(route_values),
           "routed_records_without_values": values_null,
           "routed_records_values_unreadable_count": len(values_unreadable),
           "routed_records_values_unreadable": values_unreadable,
           "statements_leaf_gets_over_bound_count": len(over_bound),
           "statements_leaf_gets_over_bound": sorted(over_bound, key=str),
           "parts_beyond_ceiling_total": sum(v["parts_beyond_ceiling"] for v in route_values)}
    dump(out_path, doc)
    print(json.dumps({**{k: v for k, v in doc.items() if k != "routed_values"},
                      "routed_records_values_unreadable": [d["entry"] for d in values_unreadable],
                      "entries_multi_hash": [d["entry"] for d in multi],
                      "entries_differing": [d["entry"] for d in differing],
                      "statements_status_differing": [d["entry"] for d in status_differing]}))
elif op == "stage1-inspect":
    # `catalog inspect --signal metrics` text, its file name for the stamp,
    # the command as run: the snapshot HEAD's watermark hour, its part count,
    # and how many parts carry at least one label_value key-index leaf ref.
    # Exit 3 and why on stdout when there is no HEAD or the text does not
    # read as one HEAD with that many parts, each with its key_index line.
    inspect_path, name, cmd = args
    try:
        with open(inspect_path) as f:
            lines = f.read().splitlines()
    except OSError as e:
        print(f"{name} could not be read ({type(e).__name__})")
        sys.exit(3)
    absent = [l for l in lines if re.match(r"^HEAD at \S+ is absent or unreadable: ", l)]
    if absent:
        print(f"no snapshot HEAD: {absent[0]}")
        sys.exit(3)
    def header(key):
        hits = [m.group(1) for m in (re.match(rf"^{key}: ([0-9]+)$", l) for l in lines) if m]
        if len(hits) != 1:
            print(f"{name} carries {len(hits)} '{key}:' lines, want 1")
            sys.exit(3)
        return int(hits[0])
    watermark, parts = header("watermark_hour"), header("parts")
    per_part = []
    for l in lines:
        if re.match(r"^  part\[[0-9]+\] ", l):
            per_part.append({"refs": None, "leaves": 0, "label_value": 0})
            continue
        m = re.match(r"^    key_index \(field 8\): (ABSENT|[0-9]+)$", l)
        if m and per_part and per_part[-1]["refs"] is None:
            per_part[-1]["refs"] = 0 if m.group(1) == "ABSENT" else int(m.group(1))
            continue
        m = re.match(r"^      field=(\S+) slice=", l)
        if m and per_part:
            per_part[-1]["leaves"] += 1
            per_part[-1]["label_value"] += m.group(1) == "label_value"
    if len(per_part) != parts:
        print(f"{name} lists {len(per_part)} parts, its parts line says {parts}")
        sys.exit(3)
    if any(p["refs"] is None or p["refs"] != p["leaves"] for p in per_part):
        print(f"{name}: a part's key_index line is missing or does not match its leaf lines")
        sys.exit(3)
    print(json.dumps({"source": f"{cmd}: the snapshot HEAD's watermark_hour, parts and per-part key_index (field 8) leaf refs, all fields of the HEAD object itself (the command also GETs every part object, whose figures are not used here)",
                      "file": name, "watermark_hour": watermark, "parts": parts,
                      "label_value_leaf_parts": sum(1 for p in per_part if p["label_value"] > 0),
                      "label_value_leaf_refs": sum(p["label_value"] for p in per_part)}))
elif op == "stage1-control-image":
    # The control image's image-inspect line and source commit, the main
    # image's reference and source commit; the main image's resolved figures
    # come from stamps.json.
    stamps_path, control_resolved, control_commit, main_ref, main_commit = args
    main = load(stamps_path).get("ravel_image")
    print(json.dumps({"control": {**json.loads(control_resolved), "source_commit": control_commit},
                      "main": {"reference": main_ref,
                               "resolved": main.get("resolved") if isinstance(main, dict) else None,
                               "source_commit": main_commit}}))
elif op == "stage1-control-compare":
    # The unrouted and the control pass's diagnostics and the output path.
    # Per entry and per pass, over the records whose status is ok or
    # incorrect and whose every phase carries s3GetRequests and
    # s3GetWireBytes: the median latency_ms, the median per-record GETs and
    # wire bytes summed over phases, and each phase's medians. Prints the
    # summary; exit 3 and why on stdout when a diagnostics file cannot be
    # read.
    unrouted_path, control_path, out_path = args
    try:
        passes = {"unrouted": cost_rows(unrouted_path), "control": cost_rows(control_path)}
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"the control diagnostics could not be read ({type(e).__name__}: {e})")
        sys.exit(3)
    def summarise(rows):
        by, unreadable = {}, 0
        for r in rows:
            cost, lat = record_cost(r), r.get("latency_ms")
            if r.get("status") not in ("ok", "incorrect") or cost is None or \
                    isinstance(lat, bool) or not isinstance(lat, (int, float)):
                unreadable += 1
                continue
            e = by.setdefault(r.get("entry"), {"latency": [], "gets": [], "bytes": [], "phases": {}})
            e["latency"].append(lat)
            e["gets"].append(sum(v[0] for v in cost.values()))
            e["bytes"].append(sum(v[1] for v in cost.values()))
            for name, (g, b) in cost.items():
                ph = e["phases"].setdefault(name, {"gets": [], "bytes": []})
                ph["gets"].append(g)
                ph["bytes"].append(b)
        return {k: {"records": len(v["latency"]), "latency_ms_median": median(v["latency"]),
                    "latency_ms_spread": max(v["latency"]) - min(v["latency"]),
                    "s3_get_requests_median": median(v["gets"]), "s3_get_wire_bytes_median": median(v["bytes"]),
                    "phases": {n: {"s3_get_requests_median": median(p["gets"]),
                                   "s3_get_wire_bytes_median": median(p["bytes"])}
                               for n, p in sorted(v["phases"].items())}}
                for k, v in by.items()}, unreadable
    u, u_bad = summarise(passes["unrouted"])
    c, c_bad = summarise(passes["control"])
    entries, more_gets, more_bytes, unchecked, ratios, slower = [], [], [], [], [], []
    for entry in sorted(set(u) | set(c) | {r.get("entry") for rows in passes.values() for r in rows}, key=str):
        a, b = u.get(entry), c.get(entry)
        if a is None or b is None:
            unchecked.append(entry)
            entries.append({"entry": entry, "unrouted": a, "control": b, "latency_ratio": None})
            continue
        ratio = a["latency_ms_median"] / b["latency_ms_median"] if b["latency_ms_median"] > 0 else None
        if ratio is None:
            unchecked.append(entry)
        else:
            ratios.append(ratio)
        if a["s3_get_requests_median"] > b["s3_get_requests_median"]:
            more_gets.append(entry)
        if a["s3_get_wire_bytes_median"] > b["s3_get_wire_bytes_median"]:
            more_bytes.append(entry)
        excess = a["latency_ms_median"] - b["latency_ms_median"]
        if excess > 0.05 * b["latency_ms_median"] and \
                excess > max(a["latency_ms_spread"], b["latency_ms_spread"]):
            slower.append(entry)
        entries.append({"entry": entry, "unrouted": a, "control": b, "latency_ratio": ratio})
    doc = {"schema": "metricsbench-stage1-control-compare/1",
           "method": "per entry, the median over its answered (ok or incorrect) records of each pass; GETs and wire bytes are each record's s3GetRequests and s3GetWireBytes summed over its phases; latency_ratio is the unrouted median latency_ms over the control's; latency_ms_spread is the max minus the min of a pass's latency_ms; an entry is slower when its unrouted median latency_ms exceeds the control's by more than 5% of the control's and by more than the larger of the two passes' spreads; an entry with no readable record in either pass, or a control median of 0 ms, is unchecked",
           "statements_compared": len(entries) - len(unchecked),
           "statements_more_gets_count": len(more_gets), "statements_more_gets": more_gets,
           "statements_more_bytes_count": len(more_bytes), "statements_more_bytes": more_bytes,
           "statements_unchecked_count": len(unchecked), "statements_unchecked": unchecked,
           "statements_slower_count": len(slower), "statements_slower": slower,
           "records_unreadable": {"unrouted": u_bad, "control": c_bad},
           "latency_ratio_median": median(ratios), "latency_ratio_max": max(ratios) if ratios else None,
           "entries": entries}
    dump(out_path, doc)
    print(json.dumps({k: v for k, v in doc.items() if k != "entries"}))
elif op == "stage1-hash-compare":
    # A base and an other pass's diagnostics, the output path and the two
    # passes' names. Per entry, the two passes' result_hash sets compared as
    # stage1-compare does; the other pass's records counted by status and
    # by label-index prunes; and the entries both passes answered whose
    # segmentsPrunedByLabelIndex figures differ between them. Prints the
    # summary; exit 3 and why on stdout when a diagnostics file cannot be
    # read.
    base_path, other_path, out_path, base_name, other_name = args
    try:
        base, other = cost_rows(base_path), cost_rows(other_path)
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"the {base_name} or {other_name} diagnostics could not be read ({type(e).__name__}: {e})")
        sys.exit(3)
    a, b = hash_sets(base), hash_sets(other)
    equal, differing, without = [], [], []
    for entry in sorted(set(a) | set(b), key=str):
        x, y = a.get(entry, set()), b.get(entry, set())
        if not x or not y:
            without.append(entry)
        elif x == y:
            equal.append(entry)
        else:
            differing.append({"entry": entry, base_name: sorted(x), other_name: sorted(y)})
    def pruned(rec):
        v = rec.get("segmentsPrunedByLabelIndex")
        return v if whole(v) else None
    figures = [pruned(r) for r in other]
    def prunes(rows, entry):
        # A record with no whole figure sorts as -1, so it never equals one.
        return sorted(-1 if pruned(r) is None else pruned(r) for r in rows if r.get("entry") == entry)
    answered = [e for e in sorted(set(a) | set(b), key=str) if a.get(e) and b.get(e)]
    prune_mismatch = [{"entry": e, base_name: prunes(base, e), other_name: prunes(other, e)}
                      for e in answered if prunes(base, e) != prunes(other, e)]
    doc = {"schema": "metricsbench-stage1-hash-compare/1", "base": base_name, "other": other_name,
           "entries_compared": len(equal) + len(differing), "entries_equal": len(equal),
           "entries_differing_count": len(differing), "entries_differing": differing,
           "entries_without_hash_count": len(without), "entries_without_hash": without,
           "other_records": len(other),
           "other_records_status_unexpected": sum(1 for r in other if r.get("status") not in ("ok", "incorrect")),
           "other_records_label_index_null": sum(1 for v in figures if v is None),
           "other_records_pruned_by_label_index": sum(1 for v in figures if v is not None and v > 0),
           "label_index_prune_mismatch_count": len(prune_mismatch), "label_index_prune_mismatch": prune_mismatch,
           "label_index_prune_basis": "per entry with a result_hash in both passes, the sorted segmentsPrunedByLabelIndex of its records in each pass, a record with no whole figure as -1; an entry whose two lists differ is a mismatch"}
    dump(out_path, doc)
    print(json.dumps({**doc, "entries_differing": [d["entry"] for d in differing]}))
elif op == "stage1-extra-hour":
    # The leaf-off and leaf-on fold reports, their wall times, the sha256 of
    # the HEAD each fold started from and of the leaf-on fold's HEAD put back
    # after both, the inspect text and catalog listing taken after that, and
    # the inspect text of the routed HEAD the leaf-on fold started from.
    # Prints the stamp; exit 3 and why on stdout when an input cannot be
    # read or lacks a field.
    off_path, on_path, off_ms, on_ms, off_head, on_head, end_head, inspect_path, list_path, base_inspect_path = args
    want = ["watermark_hour", "previous_watermark_hour", "buckets_folded", "entry_count", "put_requests",
            "key_index_leaves_written", "key_index_leaf_bytes_written", "key_index_uncovered_entries",
            "key_index_switch_off_over_leaves"]
    try:
        off, on = load(off_path), load(on_path)
        assert isinstance(off, dict) and isinstance(on, dict)
        listed = listed_objects(list_path)
        parts = inspect_parts(inspect_path)
        base_parts = inspect_parts(base_inspect_path)
    except (OSError, ValueError, AssertionError) as e:
        print(f"an extra-hour input could not be read ({type(e).__name__}: {e})")
        sys.exit(3)
    missing = [f"{n}.{k}" for n, rep in (("off", off), ("on", on)) for k in want if k not in rep]
    if missing:
        print("the extra-hour fold reports lack " + ", ".join(missing))
        sys.exit(3)
    prev = on["previous_watermark_hour"]
    if not whole(prev) or not whole(on["watermark_hour"]):
        print(f"the leaf-on fold's watermark_hour {on['watermark_hour']!r} or previous_watermark_hour {prev!r} is not a whole number")
        sys.exit(3)
    def in_hours(o, lo, hi):
        return whole(o.get("ingest_hour_bucket")) and lo <= o["ingest_hour_bucket"] <= hi
    def data_bytes(objs):
        return sum(o["object_size"] for o in objs if whole(o.get("object_size")))
    tail_objects = [o for o in listed if in_hours(o, prev + 1, on["watermark_hour"])]
    # The parts the leaf-on fold wrote: every part its HEAD carries that the
    # routed HEAD it started from does not, by key and blake3. A re-encoded
    # part whose hours all lie at or below prev is one of them.
    base_ids = {(p["key"], p["blake3"]) for p in base_parts}
    tail_parts = [p for p in parts if (p["key"], p["blake3"]) not in base_ids]
    repartitioned = [p for p in tail_parts if p["hi"] <= prev]
    # Each listed object once, however many tail parts' ranges hold its hour.
    covered = [o for o in listed if any(in_hours(o, p["lo"], p["hi"]) for p in tail_parts)]
    covered_bytes = data_bytes(covered)
    uncovered = [p["key"] for p in tail_parts
                 if not any(l["field"] == "label_value" for l in p["leaves"]) or any(l["uncovered"] > 0 for l in p["leaves"])]
    leaf_bytes = on["key_index_leaf_bytes_written"]
    off_ms, on_ms = int(off_ms), int(on_ms)
    print(json.dumps({
        "method": "ravel-server stopped; the routed HEAD saved after stage1-leaves put back and read back, catalog fold --writers-stopped --key-index-leaves --metrics-label-index timed from it, its HEAD saved; the HEAD saved before stage1-leaves (no leaf ever built on it) put back and read back, catalog fold --writers-stopped with no leaf flag timed from it; the leaf-on fold's HEAD put back and read back. The catalog HEAD is the one mutable catalog object; parts, leaves and data objects are immutable and content-keyed. The leaf-on fold runs first, so an object it wrote that the leaf-off fold would write with the same content is already there for the leaf-off fold, which can only make the leaf-off fold cheaper: wall_ratio_on_off errs high",
        "start_mismatches": int(off["previous_watermark_hour"] != prev),
        "head_sha256": {"fold_off_start": off_head, "fold_on_start": on_head, "after": end_head},
        "previous_watermark_hour": prev, "watermark_hour": on["watermark_hour"],
        "fold_off": {"wall_ms": off_ms, **{k: off[k] for k in want}},
        "fold_on": {"wall_ms": on_ms, **{k: on[k] for k in want}},
        "wall_ratio_on_off": on_ms / off_ms if off_ms > 0 else None,
        "tail_objects": len(tail_objects), "tail_data_bytes": data_bytes(tail_objects),
        "leaf_bytes": leaf_bytes,
        "leaf_bytes_denominator_objects": len(covered), "leaf_bytes_denominator": covered_bytes,
        "leaf_bytes_ratio": leaf_bytes / covered_bytes if covered_bytes > 0 else None,
        "leaf_bytes_basis": "fold_on.key_index_leaf_bytes_written (leaf_bytes) over the object_size of the listed objects whose ingest_hour_bucket lies in the lo..=hi range of some tail part, each object counted once (leaf_bytes_denominator); a tail part is a part of the leaf-on fold's HEAD whose key and blake3 the routed HEAD it started from (stage1-routed.inspect.txt) does not carry, so every part that fold wrote, a re-encoded part over older hours included, and a part's leaves index all its hours, not only the new one",
        "tail_parts": len(tail_parts), "repartitioned_parts": len(repartitioned),
        "repartitioned_parts_keys": [p["key"] for p in repartitioned],
        "uncovered_tail_parts_count": len(uncovered),
        "uncovered_tail_parts": uncovered,
        "uncovered_basis": "tail parts (as for leaf_bytes_basis) that carry no label_value leaf or a leaf with uncovered_count above 0; repartitioned_parts counts the tail parts whose range ends at or below previous_watermark_hour"}))
elif op == "stage1-compact":
    # `maintain compact-tenant` text output, its wall time, the command:
    # every counter it prints once at the start of a line. Exit 3 and why on
    # stdout when a counter is absent or repeated.
    out_path, wall_ms, cmd = args
    keys = ["compacted", "already", "rewrite_present", "not_sealed", "below_min", "tombstoned",
            "claim_skipped", "claim_cancelled", "held_out_of_window", "geometry_unsupported",
            "parts (wrote)", "wall_time_ms", "failed"]
    try:
        with open(out_path) as f:
            lines = f.read().splitlines()
    except OSError as e:
        print(f"{os.path.basename(out_path)} could not be read ({type(e).__name__})")
        sys.exit(3)
    stamp = {"command": cmd, "wall_ms": int(wall_ms)}
    for k in keys:
        hits = [l[len(k) + 2:] for l in lines if l.startswith(k + ": ")]
        if len(hits) != 1 or not hits[0].isdigit():
            print(f"{os.path.basename(out_path)} carries {len(hits)} '{k}:' lines, want 1 whole number")
            sys.exit(3)
        stamp[k.replace(" (wrote)", "_written")] = int(hits[0])
    print(json.dumps(stamp))
elif op == "stage1-l1":
    # The catalog listing after the L1 fold: data objects per level.
    (list_path,) = args
    try:
        listed = listed_objects(list_path)
    except (OSError, ValueError) as e:
        print(f"{os.path.basename(list_path)} could not be read ({type(e).__name__})")
        sys.exit(3)
    print(json.dumps({"file": os.path.basename(list_path),
                      "l0_objects": sum(1 for o in listed if o.get("level") == "L0"),
                      "l1_objects": sum(1 for o in listed if o.get("level") == "L1"),
                      "l1_bytes": sum(o.get("object_size", 0) for o in listed
                                      if o.get("level") == "L1" and whole(o.get("object_size")))}))
elif op == "stage1-erasure-corpus":
    # The corpus, the output path, a label and a value: every vector
    # selector in each entry's promql gains LABEL!="VALUE", so the pass
    # answers what the tenant holds without that subject's series. Prints
    # the rewritten entry ids; exit 3 and why on stdout on text the lexer
    # cannot follow.
    corpus_path, out_path, label, value = args
    KEYWORDS = {"by", "without", "on", "ignoring", "group_left", "group_right", "offset", "bool",
                "and", "or", "unless", "atan2", "inf", "nan", "start", "end"}
    LIST_OPENERS = {"by", "without", "on", "ignoring", "group_left", "group_right"}
    matcher = f'{label}!="{value}"'
    def rewrite(q):
        out, i, n, list_next = [], 0, len(q), False
        def skip_ws(j):
            while j < n and q[j].isspace():
                j += 1
            return j
        while i < n:
            ch = q[i]
            if ch in "\"'`":
                j = i + 1
                while j < n and q[j] != ch:
                    j += 2 if q[j] == "\\" and ch != "`" else 1
                if j >= n:
                    raise ValueError(f"unterminated string at {i}")
                out.append(q[i:j + 1])
                i = j + 1
            elif ch == "[":
                j = q.find("]", i)
                if j < 0:
                    raise ValueError(f"unterminated range at {i}")
                out.append(q[i:j + 1])
                i = j + 1
            elif ch == "(" and list_next:
                j = q.find(")", i)
                if j < 0:
                    raise ValueError(f"unterminated label list at {i}")
                out.append(q[i:j + 1])
                i, list_next = j + 1, False
            elif ch == "{":
                j = skip_ws(i + 1)
                out.append("{" + matcher + ("" if j < n and q[j] == "}" else ","))
                i += 1
                depth_str = None
                while i < n and (q[i] != "}" or depth_str):
                    if depth_str:
                        if q[i] == "\\":
                            out.append(q[i:i + 2])
                            i += 2
                            continue
                        if q[i] == depth_str:
                            depth_str = None
                    elif q[i] in "\"'`":
                        depth_str = q[i]
                    out.append(q[i])
                    i += 1
                if i >= n:
                    raise ValueError("unterminated selector")
                out.append("}")
                i += 1
            elif ch.isalpha() or ch in "_:":
                j = i
                while j < n and (q[j].isalnum() or q[j] in "_:"):
                    j += 1
                word = q[i:j]
                out.append(word)
                k = skip_ws(j)
                m = re.match(r"[A-Za-z_]+", q[k:])
                if word.lower() in KEYWORDS:
                    list_next = word in LIST_OPENERS
                elif k < n and q[k] in "({":
                    # A function or aggregation call, or a name the next
                    # branch gives its matcher inside the braces.
                    pass
                elif m and m.group(0) in ("by", "without"):
                    # An aggregation whose grouping comes before its argument.
                    pass
                else:
                    out.append("{" + matcher + "}")
                i = j
            elif ch.isdigit() or ch == ".":
                j = i
                while j < n and (q[j].isalnum() or q[j] == "." or (q[j] in "+-" and q[j - 1] in "eE")):
                    j += 1
                out.append(q[i:j])
                i = j
            else:
                if not ch.isspace():
                    list_next = False
                out.append(ch)
                i += 1
        return "".join(out)
    try:
        doc = load(corpus_path)
        rewritten = []
        for e in doc["entries"]:
            q = rewrite(e["promql"])
            if q != e["promql"]:
                rewritten.append(e["id"])
            e["promql"] = q
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"the corpus could not be rewritten ({type(e).__name__}: {e})")
        sys.exit(3)
    dump(out_path, doc)
    print(json.dumps({"matcher": matcher, "entries": len(doc["entries"]), "rewritten": rewritten}))
elif op == "stage1-erasure-status":
    # `erase status` text (services/ravel-cli/src/erase.rs print_state) and
    # the request id: its state, deferral cause and the acknowledgement and
    # completion instants. Exit 3 and why on stdout unless the request's
    # state line is there exactly once.
    status_path, request_id = args
    try:
        with open(status_path) as f:
            lines = f.read().splitlines()
    except OSError as e:
        print(f"{os.path.basename(status_path)} could not be read ({type(e).__name__})")
        sys.exit(3)
    states = [m.group(1) for m in (re.match(rf"^{re.escape(request_id)}: (PENDING|COMPLETED|UNKNOWN)", l) for l in lines) if m]
    if len(states) != 1:
        print(f"{os.path.basename(status_path)} carries {len(states)} state lines for {request_id}, want 1")
        sys.exit(3)
    def field(name):
        hits = [l.split(":", 1)[1].strip() for l in lines if l.startswith(f"  {name}:")]
        return hits[0] if len(hits) == 1 else None
    def ns(name):
        v = field(name)
        return int(v) if v is not None and v.isdigit() else None
    print(json.dumps({"request_id": request_id, "state": states[0],
                      "deferral_cause": field("deferral cause"),
                      "requested_unix_ns": ns("requested_unix_ns"),
                      "completed_unix_ns": ns("completed_unix_ns")}))
elif op == "stage1-erasure-leaves":
    # The inspect text and the catalog listing before the erasure and after
    # its fold. erase status prints no per-bucket figure and the completion
    # record's bucket drops are empty, so the erased hours are read from the
    # listings: the rewrite writes each output under a new content-keyed L1
    # key (crates/ravel-maintain/src/erasure_rewrite.rs:1474-1482,
    # keys::l1_part_key) whose rewrite record supersedes its inputs, so an
    # erased hour is an ingest hour holding a data-object key before that
    # the listing after no longer resolves. Every pre-erasure part whose
    # lo..=hi range holds an erased hour must be gone after the fold (no part
    # after carries its key and blake3), and no leaf such a part referenced
    # may be referenced after.
    pre_path, post_path, pre_list_path, post_list_path = args
    try:
        pre, post = inspect_parts(pre_path), inspect_parts(post_path)
        pre_list, post_list = listed_objects(pre_list_path), listed_objects(post_list_path)
    except (OSError, ValueError) as e:
        print(f"an erasure-leaves input could not be read ({type(e).__name__}: {e})")
        sys.exit(3)
    if not pre or not post:
        print(f"the inspect text lists {len(pre)} parts before the erasure and {len(post)} after, want at least 1 each")
        sys.exit(3)
    bad = [n for n, rows in (("before", pre_list), ("after", post_list)) for o in rows
           if not isinstance(o.get("key"), str) or not whole(o.get("ingest_hour_bucket"))]
    if bad:
        print(f"the catalog listing {bad[0]} the erasure has a line without a key or a whole ingest_hour_bucket")
        sys.exit(3)
    keys_after = {o["key"] for o in post_list}
    keys_before = {o["key"] for o in pre_list}
    by_hour = {}
    for o in pre_list:
        by_hour.setdefault(o["ingest_hour_bucket"], set()).add(o["key"])
    erased = [{"hour": h, "keys_before": len(ks), "keys_gone": len(ks - keys_after),
               "keys_new": len({o["key"] for o in post_list if o["ingest_hour_bucket"] == h} - keys_before)}
              for h, ks in sorted(by_hour.items()) if ks - keys_after]
    hours = [e["hour"] for e in erased]
    def over(p):
        return [h for h in hours if p["lo"] <= h <= p["hi"]]
    overlapping = [p for p in pre if over(p)]
    after = {(p["key"], p["blake3"]) for p in post}
    carried = [{"key": p["key"], "blake3": p["blake3"], "range": [p["lo"], p["hi"]], "erased_hours": over(p)}
               for p in overlapping if (p["key"], p["blake3"]) in after]
    post_leaves = {l["key"]: p["key"] for p in post for l in p["leaves"]}
    stale = [{"part_before": p["key"], "part_after": post_leaves[l["key"]], "leaf": l["key"], "field": l["field"]}
             for p in overlapping for l in p["leaves"] if l["key"] in post_leaves]
    print(json.dumps({"parts_before": len(pre), "parts_after": len(post),
                      "objects_before": len(pre_list), "objects_after": len(post_list),
                      "erased_hours_count": len(erased), "erased_hours": erased,
                      "overlapping_parts_count": len(overlapping),
                      "carried_over_parts_count": len(carried), "carried_over_parts": carried,
                      "leaves_after": sum(len(p["leaves"]) for p in post),
                      "stale_leaves_count": len(stale), "stale_leaves": stale,
                      "basis": "catalog list --json before the erasure (stage1-erasure-reference.objects.jsonl) and after its fold (stage1-erasure.objects.jsonl): an erased hour is an ingest_hour_bucket holding a key before that is not listed after, since the rewrite's output is a new content-keyed L1 key that supersedes its inputs; a compaction the maintain container ran in the same window replaces keys the same way and also reads as an erased hour. catalog inspect before (stage1-erasure-reference.inspect.txt) and after (stage1-erasure.inspect.txt): an overlapping part is a part before whose lo..=hi range holds an erased hour; a carried-over part is an overlapping part whose key and blake3 a part after still carries; a stale leaf is a leaf key an overlapping part referenced that a part after still references"}))
elif op == "stage1-head-sha":
    # A directory `aws s3 cp --recursive` filled with the metrics catalog
    # HEADs: the sha256 of the one HEAD. Exit 3 and why on stdout unless
    # exactly one */catalog/m/HEAD is there.
    import hashlib
    (root,) = args
    heads = sorted(os.path.join(d, f) for d, _, files in os.walk(root) for f in files
                   if f == "HEAD" and d.endswith(os.path.join("catalog", "m")))
    if len(heads) != 1:
        print(f"{root} holds {len(heads)} metrics catalog HEADs, want 1")
        sys.exit(3)
    with open(heads[0], "rb") as f:
        print(hashlib.sha256(f.read()).hexdigest())
elif op == "stage1-holders":
    # holders.json, the unrouted and the routed pass's diagnostics, the
    # catalog listing the count read, the watermark hour of the snapshot the
    # listing was taken in, the bucket, and that snapshot's name. Per statement, every unrouted record's
    # segmentsFetched must equal unrouted_fetched_expected (the objects the
    # resolve returns plus those only a min_commit_token adds), and, for a
    # route_bound statement, every routed record's segmentsPrunedByLabelIndex
    # must equal expected_routed_prunes and its keyIndex phase must show a
    # leaf read when the resolve returns an object; prints the stamp. Exit 3
    # and why on stdout when an input cannot be read.
    holders_path, diag_path, routed_path, list_path, watermark, bucket, listing_state = args
    try:
        doc = load(holders_path)
        rows = load(diag_path)["ravel_cost"]
        routed_rows = load(routed_path)["ravel_cost"]
        with open(list_path) as f:
            listed = [json.loads(l) for l in f.read().splitlines() if l.strip()]
        statements = doc["statements"]
        assert isinstance(rows, list) and isinstance(routed_rows, list) and isinstance(statements, list)
        above = sum(1 for o in listed if o["ingest_hour_bucket"] > int(watermark))
    except (OSError, ValueError, KeyError, TypeError, AssertionError) as e:
        print(f"the holders inputs could not be read ({type(e).__name__}: {e})")
        sys.exit(3)
    def whole(v):
        return isinstance(v, int) and not isinstance(v, bool)
    def by_entry(records, field):
        out = {}
        for r in records:
            v = r.get(field)
            out.setdefault(r.get("entry"), []).append(v if whole(v) else None)
        return out
    def leaf_read(r):
        phases = r.get("phases")
        key_index = [p for p in phases if isinstance(p, dict) and p.get("phase") == "keyIndex"] \
            if isinstance(phases, list) else []
        return any((whole(p.get("s3GetRequests")) and p["s3GetRequests"] > 0)
                   or (whole(p.get("cacheHits")) and p["cacheHits"] > 0) for p in key_index)
    fetched = by_entry(rows, "segmentsFetched")
    routed_pruned = by_entry(routed_rows, "segmentsPrunedByLabelIndex")
    mismatches, unchecked, per, unbound = [], [], [], []
    prune_mismatches, prune_unchecked, leaf_unread = [], [], []
    for s in statements:
        sid, want = s.get("id"), s.get("unrouted_fetched_expected")
        resolved = s.get("named_in_window") if s.get("name_pruned") else s.get("segments_in_window")
        seen_pruned = routed_pruned.get(sid, [])
        row = {k: s.get(k) for k in ("id", "segments_in_window", "named_in_window", "name_pruned",
                                     "token_only_fetched", "unrouted_fetched_expected", "holders",
                                     "value_holders", "expected_routed_prunes", "route_bound",
                                     "route_bound_reason")}
        row["routed_pruned_by_label_index"] = sorted(set(v for v in seen_pruned if v is not None))
        per.append(row)
        if s.get("route_bound") is not True:
            unbound.append({"id": sid, "reason": s.get("route_bound_reason")})
        seen = fetched.get(sid, [])
        if not seen or None in seen or not whole(want):
            unchecked.append(sid)
        elif any(v != want for v in seen):
            mismatches.append({"id": sid, "unrouted_fetched_expected": want,
                               "named_in_window": s.get("named_in_window"),
                               "segments_in_window": s.get("segments_in_window"),
                               "token_only_fetched": s.get("token_only_fetched"),
                               "unrouted_fetched": sorted(set(seen))})
        if s.get("route_bound") is not True:
            continue
        expected_prunes = s.get("expected_routed_prunes")
        if not seen_pruned or None in seen_pruned or not whole(expected_prunes):
            prune_unchecked.append(sid)
        elif any(v != expected_prunes for v in seen_pruned):
            prune_mismatches.append({"id": sid, "expected_routed_prunes": expected_prunes,
                                     "value_holders": s.get("value_holders"),
                                     "routed_pruned_by_label_index": sorted(set(seen_pruned))})
        if whole(resolved) and resolved > 0:
            unread = sum(1 for r in routed_rows if r.get("entry") == sid and not leaf_read(r))
            if unread:
                leaf_unread.append({"id": sid, "records": unread})
    print(json.dumps({"file": "holders.json", "bucket": bucket, "method": doc.get("method"),
                      "listing_state": listing_state, "listing_watermark_hour": int(watermark),
                      "objects_listed": len(listed), "objects_above_watermark": above,
                      "objects_l0": doc.get("objects_l0"), "objects_l1": doc.get("objects_l1"),
                      "series_total": doc.get("series_total"),
                      "min_commit_tokens": doc.get("min_commit_tokens"),
                      "token_objects": len(doc.get("token_objects") or []),
                      "tokens_unmatched": doc.get("tokens_unmatched"),
                      "statements": per, "skipped": len(doc.get("skipped") or []),
                      "window_check": "segmentsFetched of every stage1-unrouted record of the statement == unrouted_fetched_expected, which is the resolved set plus token_only_fetched: the resolved set is named_in_window (the objects in the window holding a series of the statement's metric) when every selector names one metric, since the unrouted resolve prunes by that name's postings, and segments_in_window (every object in the window) when the selectors name different metrics, which the resolve does not prune by name; token_only_fetched is the listed L0 objects outside that set that a min_commit_token the query lane passed names, which the resolve adds whatever their metrics and event time; a statement with no unrouted record, a record with no whole-number segmentsFetched, or a null unrouted_fetched_expected (a token naming no listed object) is counted as unchecked instead",
                      "window_mismatches_count": len(mismatches), "window_mismatches": mismatches,
                      "window_unchecked_count": len(unchecked), "window_unchecked": unchecked,
                      "bound_check": "holders (objects in the window with a series matching a qualifying selector in full) is the bound a series-level index could reach, not the label-value router's; value_holders (objects of the resolved set whose LABEL_DICT holds, for at least one selector, its metric name and every label=\"value\" value) is the router's bound, and expected_routed_prunes is the resolved set less value_holders",
                      "routed_prune_check": "for each route_bound statement, segmentsPrunedByLabelIndex of every stage1-routed record of the statement == expected_routed_prunes; a route_bound statement with no routed record, a record with no whole-number segmentsPrunedByLabelIndex, or a null expected_routed_prunes is counted as unchecked instead; statements that are not route_bound are not compared",
                      "routed_prune_mismatches_count": len(prune_mismatches),
                      "routed_prune_mismatches": prune_mismatches,
                      "routed_prune_unchecked_count": len(prune_unchecked),
                      "routed_prune_unchecked": prune_unchecked,
                      "routed_leaf_check": "for each route_bound statement whose resolved set holds at least one object, the stage1-routed records whose keyIndex phase shows no leaf read (no s3GetRequests and no cacheHits, or no keyIndex phase at all); a statement whose resolved set is empty is not counted, since its snapshot holds only token objects, which carry no part and so no leaf to read",
                      "routed_leaf_unread_count": sum(u["records"] for u in leaf_unread),
                      "routed_leaf_unread": leaf_unread,
                      "route_bound_check": "a statement whose selectors each carry a label=\"value\" matcher with a non-empty value is route_bound; for any other the router probes only that selector's metric name and keeps every object holding it, so it carries no value_holders and is left out of every comparison with the routed pass",
                      "route_unbound_count": len(unbound), "route_unbound": unbound}))
else:
    print(f"unknown op {op}", file=sys.stderr)
    sys.exit(2)
PY

# PYLIB is imported as a module from a private directory, so its bytecode is
# compiled once per run instead of once per call.
PYLIB_DIR="$(mktemp -d "${TMPDIR:-/tmp}/metricsbench-pylib.XXXXXX")" || {
  echo "metricsbench-run: cannot create a directory for the python helpers" >&2
  exit 70
}
trap 'rm -rf "$PYLIB_DIR"' EXIT
printf '%s\n' "$PYLIB" >"${PYLIB_DIR}/mbpylib.py" || {
  echo "metricsbench-run: cannot write ${PYLIB_DIR}/mbpylib.py" >&2
  exit 70
}

py() {
  MB_PYLIB_DIR="$PYLIB_DIR" python3 -I -c 'import os, sys; sys.path.insert(0, os.environ["MB_PYLIB_DIR"]); import mbpylib' "$@"
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
case "$STAGE1" in
  "" | 0) stage1=0 ;;
  1) stage1=1 ;;
  *) usage_error "MB_STAGE1 takes 1 (on) or 0 (off), got '$STAGE1'" ;;
esac
if [ "$stage1" -eq 1 ] && [ "$diagnostic" -eq 1 ]; then
  usage_error "MB_STAGE1=1 measures the folded Ravel passes; --diagnostic-prometheus-max-samples starts no Ravel"
fi
arm_control=0 arm_extra_hour=0 arm_l1=0 arm_erasure=0
if [ -n "${MB_STAGE1_ARMS:-}" ]; then
  [ "$stage1" -eq 1 ] || usage_error "MB_STAGE1_ARMS adds Stage 1 arms and needs MB_STAGE1=1"
  IFS=',' read -r -a stage1_arm_list <<<"$MB_STAGE1_ARMS"
  for arm in "${stage1_arm_list[@]}"; do
    case "$arm" in
      extra-hour) arm_extra_hour=1 ;;
      l1) arm_l1=1 ;;
      erasure) arm_erasure=1 ;;
      *) usage_error "MB_STAGE1_ARMS takes a comma list of extra-hour, l1 and erasure, got '${arm}'" ;;
    esac
  done
fi
control_image="${MB_CONTROL_IMAGE:-}"
control_source_commit="${MB_CONTROL_SOURCE_COMMIT:-}"
if [ -n "$control_image" ] || [ -n "$control_source_commit" ]; then
  [ "$stage1" -eq 1 ] || usage_error "MB_CONTROL_IMAGE adds the Stage 1 control arm and needs MB_STAGE1=1"
  [ -n "$control_image" ] && [ -n "$control_source_commit" ] ||
    usage_error "MB_CONTROL_IMAGE and MB_CONTROL_SOURCE_COMMIT are given together or not at all"
  arm_control=1
fi

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
if [ "$stage1" -eq 1 ]; then
  case "$profile" in
    ci | cardinality) ;;
    *) refuse "MB_STAGE1=1 with --profile ${profile}: the Stage 1 lanes run on cardinality (the pre-registered run) and ci (its rehearsal) only" ;;
  esac
fi
if [ "$arm_control" -eq 1 ]; then
  [[ "$control_image" =~ @sha256:[0-9a-f]{64}$ ]] ||
    refuse "MB_CONTROL_IMAGE '$control_image' carries no @sha256: digest; the control arm compares against one fixed build, and a tag could move under it"
  [[ "$control_source_commit" =~ ^[0-9a-f]{40}$ ]] ||
    refuse "MB_CONTROL_SOURCE_COMMIT '$control_source_commit' is not a full 40-character commit id"
fi
required_bins="metricsbench_ingest metricsbench_query metricsbench_report"
[ "$stage1" -eq 0 ] || required_bins="${required_bins} metricsbench_holders"
for bin in $required_bins; do
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
# ravel.yml appends this to ravel-server's entrypoint; the Stage 1 steps
# replace it with STAGE1_UNROUTED_FLAGS, then STAGE1_ROUTED_FLAGS.
if [ "$stage1" -eq 1 ]; then
  export RAVEL_SERVER_EXTRA_FLAGS="$STAGE1_UP_FLAGS"
else
  export RAVEL_SERVER_EXTRA_FLAGS=""
fi

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
stamp_str corpus "$CORPUS"
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
stamp_set query_scrapes null
stamp_set object_spans null
stamp_set cold_ravel_diagnostic null
if [ "$stage1" -eq 1 ]; then
  stamp_set stage1.enabled true
  stamp_str stage1.server_flags_up "$STAGE1_UP_FLAGS"
  stamp_str stage1.server_flags_unrouted "$STAGE1_UNROUTED_FLAGS"
  stamp_str stage1.server_flags_routed "$STAGE1_ROUTED_FLAGS"
  for key in $STAGE1_KEYS; do
    stamp_set "stage1.${key}" null
  done
  stamp_set stage1.arms "{\"control\": $(json_bool "$arm_control"), \"extra_hour\": $(json_bool "$arm_extra_hour"), \"l1\": $(json_bool "$arm_l1"), \"erasure\": $(json_bool "$arm_erasure")}"
  for arm in $STAGE1_ARMS; do
    for key in $(stage1_arm_keys "$arm"); do
      if stage1_arm_on "$arm"; then
        stamp_set "stage1.${key}" null
      else
        stamp_str "stage1.${key}" "skipped: arm off ($(stage1_arm_switch "$arm"))"
      fi
    done
  done
else
  stamp_set stage1.enabled false
fi
stamp_set steps_completed '[]'
stamp_set failed_after_ingest '[]'
stamp_set deferred_failures '[]'
stamp_set failed_step null
stamp_set exit_code null
stamp_set teardown null

# --- teardown ----------------------------------------------------------------

STACK_UP=0
# Set by folded_readback: why its steps were skipped, or that they ran.
FOLDED_SKIPPED=""
FOLDED_RAN=0
SAMPLER_PID=""
MEMORY_DONE=0
FAILED_STEP=""
# Set by run_ingest in a full run whose ingest verification failed (a row's
# read-back failed or a row stalled): the run goes on to query as a
# diagnostic and exits 70 naming ingest at the end.
INGEST_FAILED=0
INGEST_FAILURE=""
# The wall-clock hour the main ingest ended in: the newest ingest hour a
# Stage 1 compaction waits to see sealed.
INGEST_END_HOUR=0
STAGE1_LAST_INGEST_HOUR=0
# The sha256 of the HEADs stage1_steps saves for the extra-hour arm.
STAGE1_LEAFLESS_HEAD=""
STAGE1_ROUTED_HEAD=""
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
  [ -z "$STAGE1_KEY_FILE" ] || rm -f "$STAGE1_KEY_FILE"
  [ -z "$FAILED_STEP" ] || stamp_str failed_step "$FAILED_STEP"
  stamp_set exit_code "$code"
  stamp_str ended_at "$(utc_now)"
  if [ "$code" -eq 0 ]; then
    stamp_str status passed
  else
    stamp_str status failed
  fi
  rm -rf "$PYLIB_DIR"
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
  INGEST_END_HOUR=$(($(date -u +%s) / 3600))
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

# start_step: compose start also starts a service's dependencies, which reruns
# the one-shot createbuckets (it refuses a non-empty real-S3 bucket) and
# ravel-qualify. --no-deps starts the stopped container alone, and
# --no-recreate keeps it the container the run measured before the fold.
start_step() {
  local code=0 stopped="stopped: docker compose stop ravel-server before the fold"
  compose up -d --no-deps --no-recreate ravel-server >"${out_dir}/ravel-start.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/ravel-start.log"
    stamp_str ravel_restart "${stopped}; docker compose up -d --no-deps --no-recreate ravel-server after it exited $code"
    fail_step start "docker compose up -d --no-deps --no-recreate ravel-server exited $code"
  fi
  wait_ready ravel "${RAVEL_BASE}/api/v1/status/buildinfo" start
  stamp_str ravel_restart "${stopped}; started: docker compose up -d --no-deps --no-recreate ravel-server after it, then ravel answered 200 on /api/v1/status/buildinfo"
  step_done start
}

# scrape_reason CURL_EXIT HTTP FILE: "ok" for a scrape that answered 200 with
# a body, else why not.
scrape_reason() {
  if [ "$1" -ne 0 ]; then
    echo "failed: curl exit $1"
  elif [ "$2" != "200" ]; then
    echo "failed: HTTP $2"
  elif [ ! -s "$3" ]; then
    echo "failed: empty body"
  else
    echo ok
  fi
}

# scrape_query_metrics URL FILE KEY: one of the scrapes around the query step
# into FILE, its outcome stamped as query_scrapes.KEY. A failure is stamped,
# never fatal (ADR-0927's 2026-11-04 amendment), and leaves no FILE.
scrape_query_metrics() {
  local code=0 http reason
  http="$(curl -sS --max-time 10 -o "$2" -w '%{http_code}' "$1" 2>/dev/null)" || code=$?
  reason="$(scrape_reason "$code" "$http" "$2")"
  [ "$reason" = ok ] || { rm -f "$2"; say "query scrape $3: $reason"; }
  stamp_str "query_scrapes.$3" "$reason"
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
    --ravel-metrics-url "${RAVEL_BASE}/metrics" \
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
    stamp_str object_spans "failed: no fold ran: $why"
    say "steps stop, fold, start and readback-folded skipped: $why"
    FOLDED_SKIPPED="$why"
    return 0
  fi
  stop_step
  fold_step
  start_step
  readback_folded_step
  object_spans_step
  FOLDED_RAN=1
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

# object_spans_step: <out>/ravel-object-spans.json from the entries the
# catalog inspector prints (`catalog list --json`, one record per data object
# with its level, ingest hour, size and event bounds; `catalog inspect` for
# the covering snapshot parts), read after the fold. Like query_scrapes, a
# failure is stamped as object_spans, never fatal, and leaves no file.
object_spans_step() {
  local code=0 hours line list_log="${out_dir}/ravel-object-spans.list.txt"
  local inspect_log="${out_dir}/ravel-object-spans.inspect.txt"
  local list_cmd inspect_cmd
  list_cmd="ravel-cli catalog list --tenant ${RAVEL_TENANT} --shards ${RAVEL_SHARDS} --hours HOURS --json"
  inspect_cmd="ravel-cli catalog inspect --tenant ${RAVEL_TENANT} --signal metrics"
  hours=$((($(date -u +%s) - anchor_ms / 1000) / 3600 + 2))
  list_cmd="${list_cmd/HOURS/$hours}"
  compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog list \
    --tenant "$RAVEL_TENANT" --shards "$RAVEL_SHARDS" --hours "$hours" --json \
    >"$list_log" 2>"${out_dir}/ravel-object-spans.list.log" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str object_spans "failed: ${list_cmd} exited ${code}"
    say "object spans: ${list_cmd} exited ${code}"
    return 0
  fi
  code=0
  compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog inspect \
    --tenant "$RAVEL_TENANT" --signal metrics \
    >"$inspect_log" 2>"${out_dir}/ravel-object-spans.inspect.log" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str object_spans "failed: ${inspect_cmd} exited ${code}"
    say "object spans: ${inspect_cmd} exited ${code}"
    return 0
  fi
  code=0
  line="$(py object-spans "$list_log" "$inspect_log" "${out_dir}/ravel-objects.json" \
    "${out_dir}/ravel-object-spans.json" "$RAVEL_TENANT" "$list_cmd" "$inspect_cmd")" || code=$?
  if [ "$code" -ne 0 ]; then
    rm -f "${out_dir}/ravel-object-spans.json"
    stamp_str object_spans "failed: ${line:-object-spans exited $code}"
    say "object spans: ${line:-object-spans exited $code}"
    return 0
  fi
  stamp_set object_spans "$line"
}

# ravel_ready_nonfatal: wait_ready's loop for ravel, returning 1 instead of
# failing the step.
ravel_ready_nonfatal() {
  local i code
  for ((i = 1; i <= READY_TRIES; i++)); do
    code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "${RAVEL_BASE}/api/v1/status/buildinfo" 2>/dev/null)" || code=""
    [ "$code" = "200" ] && return 0
    sleep "$READY_INTERVAL"
  done
  return 1
}

COLD_REGIME="cold-ravel-diagnostic"
COLD_REPS=5
# The one full-range entry the pass adds to the long_range class; every other
# id comes from the corpus.
COLD_EXTRA_ENTRY="mb_full_range_avg_over_time"

# cold_ravel_diagnostic: after the memory step, the corpus entries of
# class long_range plus COLD_EXTRA_ENTRY declared for the profile, each run
# COLD_REPS times through metricsbench_query, ravel-server restarted before
# every run. Reports go under <out>/cold-ravel-diagnostic/, never to
# report.json, so no row joins the timed set. A failure of any run, restart
# or write is recorded (in the file, and stamped cold_ravel_diagnostic),
# never fatal. Reads run_full's it_env, store_args and prov_tail.
cold_ravel_diagnostic() {
  cold_pass "${out_dir}/cold-ravel-diagnostic" "${out_dir}/cold-ravel-diagnostic.json" \
    cold_ravel_diagnostic "cold ravel diagnostic" "$COLD_REGIME"
}

# cold_pass DIR FINAL STAMP_KEY LABEL REGIME [QUERY_ARG ...]: the cold
# diagnostic's selection and procedure, with its runs under DIR, its result
# in FINAL, its outcome stamped as STAMP_KEY, its messages prefixed with
# LABEL, and each QUERY_ARG appended to every metricsbench_query run.
cold_pass() {
  local dir="$1" final="$2" key="$3" label="$4" regime="$5" ids id r code why line dead=""
  local runs="${1}/runs.json"
  local query_extra=("${@:6}")
  mkdir -p "$dir" || { stamp_str "$key" "failed: cannot create $dir"; return 0; }
  # cold-record appends to runs.json; a reused out-dir would otherwise carry a
  # previous pass's records into cold-final, which refuses duplicates.
  rm -f "$runs"
  code=0
  ids="$(py cold-select "$CORPUS" "$profile" "$dir" "$COLD_EXTRA_ENTRY")" || code=$?
  if [ "$code" -eq 3 ]; then
    stamp_str "$key" "not applicable: no entry of class long_range or ${COLD_EXTRA_ENTRY} in the corpus ${CORPUS} is declared for profile ${profile}"
    say "${label}: no entry selected for profile ${profile}"
    return 0
  elif [ "$code" -ne 0 ]; then
    stamp_str "$key" "failed: ${ids:-cold-select exited ${code} on the corpus ${CORPUS}}"
    say "${label}: ${ids:-cold-select exited ${code}}"
    return 0
  fi
  for id in $ids; do
    code=0
    "${it_env[@]}" GITHUB_SHA="$ravel_source_commit" "${bin_dir}/metricsbench_report" provenance \
      --workload "$WORKLOAD" --corpus "${dir}/corpus-${id}.json" "${prov_tail[@]}" \
      --config "diagnostic_regime=${regime}" --config "comparable=false" \
      >"${dir}/provenance-${id}.json" 2>>"${dir}/provenance.log" || code=$?
    for ((r = 1; r <= COLD_REPS; r++)); do
      why=""
      if [ "$code" -ne 0 ]; then
        why="metricsbench_report provenance exited ${code}"
      elif [ -n "$dead" ]; then
        # One readiness wait is READY_TRIES x READY_INTERVAL; repeating it
        # for every remaining repetition of a server that does not come back
        # would hold the fold and the folded read-back for hours.
        why="skipped after ravel did not answer 200 following ${dead}"
      elif ! compose restart --no-deps ravel-server >>"${dir}/restart.log" 2>&1; then
        # Without --no-deps, compose restart also bounces ravel-server's
        # dependency chain (ravel-qualify, createbuckets, s3s), and the cold
        # figure would include an object-store restart.
        why="docker compose restart --no-deps ravel-server failed"
      elif ! ravel_ready_nonfatal; then
        why="ravel did not answer 200 after the restart"
        dead="${id} repetition ${r}"
      fi
      if [ -z "$why" ]; then
        local qcode=0
        "${bin_dir}/metricsbench_query" --ingest-report "${out_dir}/ingest.json" --workload "$WORKLOAD" \
          --corpus "${dir}/corpus-${id}.json" --provenance "${dir}/provenance-${id}.json" \
          --engine "name=prometheus,url=${PROM_BASE}" \
          --engine "name=ravel,url=${RAVEL_BASE},bearer_env=${RAVEL_TOKEN_ENV}" \
          --warmup-passes 0 --passes 1 --reps-per-pass 1 \
          --diagnostics "${dir}/${id}.${r}.diagnostics.json" --out "${dir}/${id}.${r}.report.json" \
          ${query_extra[@]+"${query_extra[@]}"} \
          >"${dir}/${id}.${r}.query.log" 2>&1 || qcode=$?
        py cold-record "$runs" "$id" "$r" "$qcode" "${dir}/${id}.${r}.diagnostics.json" ||
          say "${label}: could not record ${id} repetition ${r}"
      else
        say "${label}: ${id} repetition ${r}: ${why}"
        py cold-record "$runs" "$id" "$r" "${why}" "${dir}/none" ||
          say "${label}: could not record ${id} repetition ${r}"
      fi
    done
  done
  code=0
  # shellcheck disable=SC2086
  line="$(py cold-final "$runs" "$final" "$profile" "$regime" "$COLD_REPS" \
    "$CORPUS" $ids)" || code=$?
  if [ "$code" -ne 0 ]; then
    rm -f "$final"
    stamp_str "$key" "failed: ${final##*/} not written: ${line:-cold-final exited ${code}}"
    say "${label}: ${line:-cold-final exited ${code}}"
    return 0
  fi
  stamp_set "$key" "$line"
}

# --- Stage 1 (MB_STAGE1=1) ---------------------------------------------------

now_ms() {
  python3 -I -c 'import time; print(int(time.time() * 1000))'
}

# stage1_catalog_fold STEP STAMP_KEY OP FILE FOLD_ARG ...: ravel-cli catalog
# fold --json with FOLD_ARGs on ravel-qualify into <out>/FILE.json, its
# report read by the py op OP and stamped as STAMP_KEY with its wall time.
stage1_catalog_fold() {
  local step="$1" key="$2" op="$3" file="$4" code=0 t0 t1 line
  shift 4
  t0="$(now_ms)"
  compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog fold \
    --tenant "$RAVEL_TENANT" --shards "$RAVEL_SHARDS" "$@" --json \
    >"${out_dir}/${file}.json" 2>"${out_dir}/${file}.log" || code=$?
  t1="$(now_ms)"
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/${file}.log"
    stamp_str "$key" "failed: ravel-cli catalog fold $* --json exited $code"
    fail_step "$step" "ravel-cli catalog fold $* --json exited $code; see ${out_dir}/${file}.log"
  fi
  code=0
  line="$(py "$op" "${out_dir}/${file}.json" "$((t1 - t0))")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str "$key" "failed: $line"
    fail_step "$step" "$line"
  fi
  stamp_set "$key" "$line"
  step_done "$step"
}

# stage1_recreate STEP STAMP_KEY FLAGS: ravel-server recreated alone with
# RAVEL_SERVER_EXTRA_FLAGS=FLAGS (compose recreates the container when its
# entrypoint changes, --force-recreate also when it does not; --no-deps keeps
# the one-shots and the object store as they are), then its readiness URL
# answers 200. Stamped as STAMP_KEY; a failure is STEP's.
stage1_recreate() {
  local step="$1" key="$2" code=0 log="${out_dir}/stage1-${2#stage1.}.log"
  export RAVEL_SERVER_EXTRA_FLAGS="$3"
  compose up -d --no-deps --force-recreate ravel-server >"$log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "$log"
    stamp_str "$key" "failed: docker compose up -d --no-deps --force-recreate ravel-server exited $code"
    fail_step "$step" "docker compose up -d --no-deps --force-recreate ravel-server exited $code"
  fi
  wait_ready ravel "${RAVEL_BASE}/api/v1/status/buildinfo" "$step"
  stamp_str "$key" "recreated: docker compose up -d --no-deps --force-recreate ravel-server with RAVEL_SERVER_EXTRA_FLAGS='$3', then ravel answered 200 on /api/v1/status/buildinfo"
}

# stage1_whole_figure WHICH PATH: the stamp at PATH read back into
# STAGE1_FIGURE. When it cannot be read, or is not a whole number, the stamp
# stage1.WHICH says so and step stage1-WHICH (underscores as dashes) fails,
# since the pass it gates cannot check its snapshot.
stage1_whole_figure() {
  local which="$1" path="$2" code=0
  STAGE1_FIGURE="$(py get "$STAMPS" "$path")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str "stage1.${which}" "refused: ${path} could not be read (py get exited ${code})"
    fail_step "stage1-${which//_/-}" "refused: ${path} could not be read (py get exited ${code}), so the ${which} pass's snapshot cannot be checked"
  fi
  if ! [[ "$STAGE1_FIGURE" =~ ^[0-9]+$ ]]; then
    stamp_str "stage1.${which}" "refused: ${path} is '${STAGE1_FIGURE}', not a whole number"
    fail_step "stage1-${which//_/-}" "refused: ${path} is '${STAGE1_FIGURE}', not a whole number, so the ${which} pass's snapshot cannot be checked"
  fi
}

# stage1_inspect STEP FILE: ravel-cli catalog inspect --signal metrics on
# ravel-qualify into <out>/FILE, with the main image's CLI whichever image
# ravel-server runs; a failure is STEP's.
stage1_inspect() {
  local code=0
  RAVEL_IMAGE="$ravel_image" compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog inspect \
    --tenant "$RAVEL_TENANT" --signal metrics \
    >"${out_dir}/$2" 2>"${out_dir}/$2.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/$2.log"
    fail_step "$1" "ravel-cli catalog inspect --tenant ${RAVEL_TENANT} --signal metrics exited ${code}; see ${out_dir}/$2.log"
  fi
}

# stage1_list STEP FILE: ravel-cli catalog list --json (the data objects the
# folded snapshot resolves, L0 and L1) into <out>/FILE; a failure is STEP's.
stage1_list() {
  local code=0 hours
  hours=$((($(date -u +%s) - anchor_ms / 1000) / 3600 + 2))
  RAVEL_IMAGE="$ravel_image" compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog list \
    --tenant "$RAVEL_TENANT" --shards "$RAVEL_SHARDS" --hours "$hours" --json \
    >"${out_dir}/$2" 2>"${out_dir}/$2.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/$2.log"
    fail_step "$1" "ravel-cli catalog list --tenant ${RAVEL_TENANT} --hours ${hours} --json exited ${code}; see ${out_dir}/$2.log"
  fi
}

# stage1_catalog_state WHICH: the snapshot the pass WHICH (unrouted, routed,
# control, l1, erasure_reference or erasure_pending) is about to read, from
# ravel-cli catalog inspect on ravel-qualify (the main image's CLI), stamped
# as stage1.WHICH_catalog. The pass is refused (its step fails) when the text
# cannot be read, when the stamp cannot be written or its
# label_value_leaf_parts, watermark_hour or parts read back as a whole
# number, when the unrouted or control pass would run with any part carrying
# a label_value leaf, or when the routed pass would run with fewer such parts
# than stage1-leaves reported rebuilt (parts_rebuilt).
stage1_catalog_state() {
  local which="$1" step="stage1-${1//_/-}" key="stage1.$1_catalog" code=0 line want
  local file="stage1-${1//_/-}.inspect.txt"
  local cmd="ravel-cli catalog inspect --tenant ${RAVEL_TENANT} --signal metrics"
  RAVEL_IMAGE="$ravel_image" compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog inspect \
    --tenant "$RAVEL_TENANT" --signal metrics \
    >"${out_dir}/${file}" 2>"${out_dir}/stage1-${1//_/-}.inspect.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/stage1-${1//_/-}.inspect.log"
    stamp_str "$key" "failed: ${cmd} exited ${code}"
    fail_step "$step" "refused: the snapshot state could not be read: ${cmd} exited ${code}"
  fi
  code=0
  line="$(py stage1-inspect "${out_dir}/${file}" "$file" "$cmd")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str "$key" "failed: ${line:-stage1-inspect exited $code}"
    fail_step "$step" "refused: the snapshot state could not be read: ${line:-stage1-inspect exited $code}"
  fi
  code=0
  stamp_set "$key" "$line" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str "stage1.${which}" "refused: ${key} could not be stamped (py set exited ${code})"
    fail_step "$step" "refused: the snapshot state could not be stamped as ${key} (py set exited ${code})"
  fi
  stage1_whole_figure "$which" "${key}.label_value_leaf_parts"
  STAGE1_LEAF_PARTS="$STAGE1_FIGURE"
  stage1_whole_figure "$which" "${key}.watermark_hour"
  STAGE1_WATERMARK="$STAGE1_FIGURE"
  stage1_whole_figure "$which" "${key}.parts"
  STAGE1_PARTS="$STAGE1_FIGURE"
  if [ "$which" = unrouted ] || [ "$which" = control ]; then
    if [ "$STAGE1_LEAF_PARTS" -ne 0 ]; then
      stamp_str "stage1.${which}" "refused: ${STAGE1_LEAF_PARTS} snapshot parts carry a label_value leaf before the ${which} pass"
      fail_step "$step" "refused: ${STAGE1_LEAF_PARTS} snapshot parts carry a label_value leaf before the ${which} pass, which must run with none"
    fi
  elif [ "$which" = routed ]; then
    stage1_whole_figure "$which" stage1.leaves.parts_rebuilt
    want="$STAGE1_FIGURE"
    if [ "$STAGE1_LEAF_PARTS" -lt "$want" ]; then
      stamp_str "stage1.${which}" "refused: ${STAGE1_LEAF_PARTS} snapshot parts carry a label_value leaf, stage1-leaves rebuilt ${want}"
      fail_step "$step" "refused: ${STAGE1_LEAF_PARTS} snapshot parts carry a label_value leaf before the routed pass, fewer than the ${want} stage1-leaves rebuilt"
    fi
  fi
}

# stage1_pass WHICH: the corpus (STAGE1_PASS_CORPUS when set) against
# Prometheus and Ravel, as the query step runs it, into <out>/stage1-WHICH/
# (underscores as dashes throughout). For unrouted, routed and control, then
# the cold pass's selection into <out>/stage1-WHICH/cold.json, and the cold
# records with a ravel_cost counted against the selected entries x
# repetitions as stage1.WHICH_cold_records (0 of 0 when the cold stamp says
# not applicable; a string naming why when there is no cold stamp to count).
# Every ravel_cost record is labelled pass_label folded-WHICH with the fold
# state stage1_catalog_state read just before it.
stage1_pass() {
  local which="$1" name="${1//_/-}" code=0 state flags line corpus="${STAGE1_PASS_CORPUS:-$CORPUS}"
  local label="folded-${name}" step="stage1-${name}" dir="${out_dir}/stage1-${name}"
  stage1_catalog_state "$which"
  case "$which" in
    unrouted | control) flags="$STAGE1_UNROUTED_FLAGS" ;;
    *) flags="$STAGE1_ROUTED_FLAGS" ;;
  esac
  state="folded: watermark_hour=${STAGE1_WATERMARK}; parts=${STAGE1_PARTS}; label_value leaf parts=${STAGE1_LEAF_PARTS}; ravel-server ${flags}"
  [ "$which" != control ] || state="${state}; image ${control_image}"
  mkdir -p "$dir" || fail_step "$step" "cannot create $dir"
  "${it_env[@]}" GITHUB_SHA="$ravel_source_commit" "${bin_dir}/metricsbench_report" provenance \
    --workload "$WORKLOAD" --corpus "$corpus" "${prov_tail[@]}" \
    --config "diagnostic_regime=stage1-${label}" --config "comparable=false" \
    >"${dir}/provenance.json" 2>"${dir}/provenance.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${dir}/provenance.log"
    fail_step "$step" "metricsbench_report provenance exited $code"
  fi
  local query_args=(--ingest-report "${out_dir}/ingest.json" --workload "$WORKLOAD" --corpus "$corpus"
    --provenance "${dir}/provenance.json"
    --engine "name=prometheus,url=${PROM_BASE}"
    --engine "name=ravel,url=${RAVEL_BASE},bearer_env=${RAVEL_TOKEN_ENV}"
    --pass-label "$label" --fold-state "$state"
    --diagnostics "${dir}/diagnostics.json"
    --out "${dir}/report.json")
  [ -z "$passes" ] || query_args+=(--passes "$passes")
  [ -z "$reps" ] || query_args+=(--reps-per-pass "$reps")
  code=0
  "${bin_dir}/metricsbench_query" "${query_args[@]}" >"${dir}/query.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${dir}/query.log"
    fail_step "$step" "metricsbench_query exited $code"
  fi
  [ -s "${dir}/diagnostics.json" ] || fail_step "$step" "metricsbench_query exited 0 and wrote no diagnostics.json"
  stamp_set "stage1.${which}" "$(python3 -I -c 'import json,sys; print(json.dumps(dict(zip(["pass_label","fold_state","diagnostics","report"], sys.argv[1:]))))' \
    "$label" "$state" "stage1-${name}/diagnostics.json" "stage1-${name}/report.json")"
  case "$which" in
    unrouted | routed | control) ;;
    *)
      step_done "$step"
      return 0
      ;;
  esac
  cold_pass "${dir}/cold" "${dir}/cold.json" "stage1.${which}_cold" "stage1 ${which} cold" \
    "stage1-${label}-cold" --pass-label "$label" --fold-state "$state"
  code=0
  line="$(py cold-count "$STAMPS" "stage1.${which}_cold")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str "stage1.${which}_cold_records" "failed: ${line:-cold-count exited $code}"
    say "stage1 ${which} cold records: ${line:-cold-count exited $code}"
  else
    stamp_set "stage1.${which}_cold_records" "$line"
  fi
  step_done "$step"
}

# stage1_compare: <out>/stage1-compare.json from the two steady passes'
# diagnostics; stamped, never fatal (the band file is the verdict).
stage1_compare() {
  local code=0 line
  line="$(py stage1-compare "${out_dir}/stage1-unrouted/diagnostics.json" \
    "${out_dir}/stage1-routed/diagnostics.json" "${out_dir}/stage1-compare.json")" || code=$?
  if [ "$code" -ne 0 ]; then
    rm -f "${out_dir}/stage1-compare.json"
    stamp_str stage1.compare "failed: ${line:-stage1-compare exited $code}"
    say "stage1 compare: ${line:-stage1-compare exited $code}"
    return 0
  fi
  stamp_set stage1.compare "$line"
}

# stage1_holders_step: the data objects the catalog resolves for the folded
# snapshot (`catalog list --json`, L0 and L1) listed into
# <out>/stage1-objects.jsonl; the bucket's L0 and L1 data objects downloaded
# through the createbuckets service (each lane's own endpoint and
# credentials), as the invoking user so the files can be deleted; the listed
# ones counted by metricsbench_holders without the router, per statement over
# the objects its fetch window overlaps and the commit tokens the query lane
# passed. The download is removed after. The stamp checks each unrouted
# record's segmentsFetched against the statement's unrouted_fetched_expected,
# each routed record of a route-bound statement's segmentsPrunedByLabelIndex
# against its expected_routed_prunes and its keyIndex phase for a leaf read,
# and lists the statements that are not route-bound.
stage1_holders_step() {
  local bucket code=0 dest="${out_dir}/stage1-objects" hours line
  local list_file="${out_dir}/stage1-objects.jsonl"
  local list_cmd="ravel-cli catalog list --tenant ${RAVEL_TENANT} --shards ${RAVEL_SHARDS} --hours HOURS --json"
  local sync_args=(s3 sync)
  hours=$((($(date -u +%s) - anchor_ms / 1000) / 3600 + 2))
  list_cmd="${list_cmd/HOURS/$hours}"
  compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed catalog list \
    --tenant "$RAVEL_TENANT" --shards "$RAVEL_SHARDS" --hours "$hours" --json \
    >"$list_file" 2>"${out_dir}/stage1-holders.list.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/stage1-holders.list.log"
    stamp_str stage1.holders "failed: ${list_cmd} exited $code"
    fail_step stage1-holders "${list_cmd} exited $code; see ${out_dir}/stage1-holders.list.log"
  fi
  code=0
  if [ "$real_s3" -eq 1 ]; then
    bucket="${real_s3_prefix}-ravel"
  else
    bucket="ravel-metricsbench"
    sync_args=(--endpoint-url http://s3s:9000 "${sync_args[@]}")
  fi
  rm -rf "$dest"
  mkdir -p "$dest" || fail_step stage1-holders "cannot create $dest"
  compose run --rm --no-deps -T --user "$(id -u):$(id -g)" -e HOME=/tmp -v "${dest}:/stage1-objects" \
    --entrypoint aws createbuckets "${sync_args[@]}" "s3://${bucket}/t/" /stage1-objects/t/ \
    --exclude '*' --include '*/m/l0/*.rseg' --include '*/m/l1/*.rseg' --only-show-errors \
    >"${out_dir}/stage1-holders.download.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    rm -rf "$dest"
    show_log "${out_dir}/stage1-holders.download.log"
    stamp_str stage1.holders "failed: aws s3 sync of s3://${bucket}/t/ exited $code"
    fail_step stage1-holders "aws s3 sync of s3://${bucket}/t/ exited $code; see ${out_dir}/stage1-holders.download.log"
  fi
  code=0
  "${bin_dir}/metricsbench_holders" --corpus "$CORPUS" --ingest-report "${out_dir}/ingest.json" \
    --workload "$WORKLOAD" --objects-list "$list_file" --objects-dir "$dest" \
    --out "${out_dir}/holders.json" >"${out_dir}/stage1-holders.log" 2>&1 || code=$?
  rm -rf "$dest"
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/stage1-holders.log"
    stamp_str stage1.holders "failed: metricsbench_holders exited $code"
    fail_step stage1-holders "metricsbench_holders exited $code; see ${out_dir}/stage1-holders.log"
  fi
  code=0
  # The listing is taken after stage1-routed, so it is the routed snapshot's
  # objects and is compared against that snapshot's watermark.
  stage1_whole_figure holders stage1.routed_catalog.watermark_hour
  line="$(py stage1-holders "${out_dir}/holders.json" "${out_dir}/stage1-unrouted/diagnostics.json" \
    "${out_dir}/stage1-routed/diagnostics.json" "$list_file" "$STAGE1_FIGURE" "$bucket" \
    "routed: listed after stage1-routed, objects_above_watermark against stage1.routed_catalog.watermark_hour")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.holders "failed: ${line:-stage1-holders exited $code}"
    fail_step stage1-holders "${line:-stage1-holders exited $code}"
  fi
  stamp_set stage1.holders "$line"
  step_done stage1-holders
}

# stage1_note KEY OP ARG...: the py op's line stamped as KEY, or why it failed;
# never fatal (the band file is the verdict).
stage1_note() {
  local key="$1" op="$2" code=0 line
  shift 2
  line="$(py "$op" "$@")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str "$key" "failed: ${line:-${op} exited $code}"
    say "${key}: ${line:-${op} exited $code}"
    return 0
  fi
  stamp_set "$key" "$line"
}

# stage1_wait_until STEP EPOCH_S WHY: METRICSBENCH_SLEEP (sleep) until the
# wall clock reaches EPOCH_S.
stage1_wait_until() {
  local now
  now="$(date -u +%s)"
  [ "$now" -lt "$2" ] || return 0
  say "$1: waiting $(($2 - now))s: $3"
  "$SLEEP" "$(($2 - now))" || fail_step "$1" "${SLEEP} $(($2 - now)) failed"
}

# stage1_stop STEP: ravel-server stopped, so a --writers-stopped fold or a
# compaction runs with no writer.
stage1_stop() {
  local code=0 log="${out_dir}/${1}.stop.log"
  compose stop ravel-server >"$log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "$log"
    fail_step "$1" "docker compose stop ravel-server exited $code"
  fi
}

# stage1_s3 STEP LOG ARG...: aws ARGs on the createbuckets service (each
# lane's own endpoint and credentials), as the invoking user, with
# <out>/stage1-s3 mounted at /stage1-s3; a failure is STEP's.
stage1_s3() {
  local step="$1" log="$2" code=0 endpoint=()
  shift 2
  [ "$real_s3" -eq 1 ] || endpoint=(--endpoint-url http://s3s:9000)
  mkdir -p "${out_dir}/stage1-s3" || fail_step "$step" "cannot create ${out_dir}/stage1-s3"
  compose run --rm --no-deps -T --user "$(id -u):$(id -g)" -e HOME=/tmp -v "${out_dir}/stage1-s3:/stage1-s3" \
    --entrypoint aws createbuckets ${endpoint[@]+"${endpoint[@]}"} "$@" >"$log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "$log"
    fail_step "$step" "aws $1 $2 exited $code; see $log"
  fi
}

stage1_bucket() {
  if [ "$real_s3" -eq 1 ]; then echo "${real_s3_prefix}-ravel"; else echo ravel-metricsbench; fi
}

# stage1_head_fetch STEP NAME: the tenant's metrics catalog HEAD downloaded
# under <out>/stage1-s3/NAME/ and its sha256 printed into STAGE1_HEAD_SHA.
stage1_head_fetch() {
  local code=0 bucket
  bucket="$(stage1_bucket)"
  rm -rf "${out_dir}/stage1-s3/$2"
  stage1_s3 "$1" "${out_dir}/${1}.$2.log" s3 cp --recursive "s3://${bucket}/t/" "/stage1-s3/$2/t/" \
    --exclude '*' --include '*/catalog/m/HEAD' --only-show-errors
  STAGE1_HEAD_SHA="$(py stage1-head-sha "${out_dir}/stage1-s3/$2")" || code=$?
  [ "$code" -eq 0 ] || fail_step "$1" "${STAGE1_HEAD_SHA:-stage1-head-sha exited $code}"
}

# stage1_head_restore STEP NAME SHA: the HEAD stage1_head_fetch saved under
# NAME put back with `aws s3 cp`, then read back; a read-back whose sha256 is
# not SHA (the saved one) refuses STEP and stamps stage1.extra_hour.
stage1_head_restore() {
  local bucket
  bucket="$(stage1_bucket)"
  stage1_s3 "$1" "${out_dir}/${1}.restore-$2.log" \
    s3 cp --recursive "/stage1-s3/$2/t/" "s3://${bucket}/t/" --only-show-errors
  stage1_head_fetch "$1" "$2-readback"
  if [ "$STAGE1_HEAD_SHA" != "$3" ]; then
    stamp_str stage1.extra_hour "refused: the restored ${2} HEAD reads back as sha256 ${STAGE1_HEAD_SHA}, the saved one is ${3}"
    fail_step "$1" "the restored ${2} HEAD reads back as sha256 ${STAGE1_HEAD_SHA}, not the saved ${3}"
  fi
}

# stage1_control_step: ADR-3694 row 12. ravel-server recreated from
# MB_CONTROL_IMAGE with the unrouted flags on the snapshot the unrouted pass
# read; both images' digests and source commits stamped; the corpus run as
# stage1-control, steady and cold; ravel-server recreated from the main
# image; <out>/stage1-control-compare.json compares the unrouted pass with
# the control per statement (latency ratio, GETs and wire bytes per phase).
stage1_control_step() {
  local code=0 resolved
  export RAVEL_IMAGE="$control_image"
  stage1_recreate stage1-control stage1.control_restart "$STAGE1_UNROUTED_FLAGS"
  resolved="$("$DOCKER" image inspect "$control_image" 2>/dev/null | py image-inspect "$control_image")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.control_image "failed: docker image inspect does not report the digest of ${control_image} (got: ${resolved:-no such image})"
    fail_step stage1-control "docker image inspect does not report the digest of ${control_image}"
  fi
  stamp_set stage1.control_image "$(py stage1-control-image "$STAMPS" "$resolved" "$control_source_commit" \
    "$ravel_image" "$ravel_source_commit")"
  stage1_pass control
  export RAVEL_IMAGE="$ravel_image"
  stage1_recreate stage1-control-restore stage1.control_restore "$STAGE1_UNROUTED_FLAGS"
  step_done stage1-control-restore
  stage1_note stage1.control_compare stage1-control-compare "${out_dir}/stage1-unrouted/diagnostics.json" \
    "${out_dir}/stage1-control/diagnostics.json" "${out_dir}/stage1-control-compare.json"
}

# stage1_extra_hour_step: ADR-3694 row 7. Once the wall clock is past the
# routed snapshot's watermark hour (and the next hour of the profile's scrape
# grid is no more than 5 minutes ahead of it), metricsbench_ingest writes
# that hour to Ravel alone. ravel-server is stopped. The routed HEAD
# stage1_steps saved is put back and read back, and a --writers-stopped fold
# with --key-index-leaves --metrics-label-index is timed from it; its HEAD is
# saved. The leafless HEAD stage1_steps saved before stage1-leaves is put
# back and read back, and the same fold with no leaf switch is timed from it,
# refused unless it reports no key-index switch-off over leaves. The leaf-on
# fold's HEAD is then put back and read back, so the later steps read the
# snapshot the leaf-on fold published.
stage1_extra_hour_step() {
  local dir="${out_dir}/stage1-extra-hour" code=0 line watermark target extra_anchor steps end_s
  local fold_on_head off_ms on_ms switch_off
  mkdir -p "$dir" || fail_step stage1-extra-hour-ingest "cannot create $dir"
  stage1_whole_figure extra_hour stage1.routed_catalog.watermark_hour
  watermark="$STAGE1_FIGURE"
  extra_anchor=$((anchor_ms + duration_secs * 1000))
  steps=$((3600 / interval_secs))
  end_s=$(((extra_anchor + (steps - 1) * interval_secs * 1000) / 1000))
  target=$(((watermark + 1) * 3600 + 60))
  [ $((end_s - 300)) -le "$target" ] || target=$((end_s - 300))
  stage1_wait_until stage1-extra-hour-ingest "$target" \
    "the extra hour must land in an ingest hour above watermark_hour ${watermark}, and its last sample no more than 5 minutes ahead"
  "${bin_dir}/metricsbench_ingest" --workload "$WORKLOAD" --profile "$profile" \
    --anchor-ms "$extra_anchor" --steps "$steps" --no-in-process-row --tenant "$RAVEL_TENANT" \
    --max-ingest-secs "$max_ingest_secs" --readback-timeout-secs "$readback_timeout_secs" \
    --ravel-endpoint "${RAVEL_BASE}/api/v1/write" --ravel-tenant "$RAVEL_TENANT" \
    --ravel-query-url "$RAVEL_BASE" --ravel-bearer-token-env "$RAVEL_TOKEN_ENV" \
    >"${dir}/ingest.json" 2>"${dir}/ingest.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${dir}/ingest.log"
    stamp_str stage1.extra_hour_ingest "failed: metricsbench_ingest exited $code"
    fail_step stage1-extra-hour-ingest "metricsbench_ingest exited $code; see ${dir}/ingest.log"
  fi
  STAGE1_LAST_INGEST_HOUR=$(($(date -u +%s) / 3600))
  stamp_set stage1.extra_hour_ingest "{\"report\": \"stage1-extra-hour/ingest.json\", \"system\": \"ravel\", \"anchor_ms\": ${extra_anchor}, \"steps\": ${steps}, \"waited_until\": ${target}, \"ingest_end_hour\": ${STAGE1_LAST_INGEST_HOUR}, \"method\": \"metricsbench_ingest --no-in-process-row with --ravel-endpoint only, the profile's next hour on its scrape grid (anchor_ms + duration_secs)\"}"
  step_done stage1-extra-hour-ingest
  stage1_stop stage1-extra-hour-fold-on
  stage1_head_restore stage1-extra-hour-fold-on head-routed "$STAGE1_ROUTED_HEAD"
  stage1_catalog_fold stage1-extra-hour-fold-on stage1.extra_hour_fold_on stage1-fold stage1-extra-hour-fold-on \
    --signal metrics --writers-stopped --key-index-leaves --metrics-label-index
  stage1_head_fetch stage1-extra-hour-fold-off head-fold-on
  fold_on_head="$STAGE1_HEAD_SHA"
  stage1_head_restore stage1-extra-hour-fold-off head-leafless "$STAGE1_LEAFLESS_HEAD"
  stage1_catalog_fold stage1-extra-hour-fold-off stage1.extra_hour_fold_off stage1-fold stage1-extra-hour-fold-off \
    --signal metrics --writers-stopped
  switch_off="$(py get "$STAMPS" stage1.extra_hour_fold_off.key_index_switch_off_over_leaves)" ||
    fail_step stage1-extra-hour "stage1.extra_hour_fold_off.key_index_switch_off_over_leaves could not be read"
  if [ "$switch_off" != False ]; then
    stamp_str stage1.extra_hour "refused: the leaf-off fold reports key_index_switch_off_over_leaves ${switch_off}, so it did not start from a leafless lineage"
    fail_step stage1-extra-hour "the leaf-off fold reports key_index_switch_off_over_leaves ${switch_off}, want false"
  fi
  stage1_head_restore stage1-extra-hour head-fold-on "$fold_on_head"
  rm -rf "${out_dir}/stage1-s3"
  stage1_inspect stage1-extra-hour stage1-extra-hour.inspect.txt
  stage1_list stage1-extra-hour stage1-extra-hour.objects.jsonl
  off_ms="$(py get "$STAMPS" stage1.extra_hour_fold_off.wall_ms)" || fail_step stage1-extra-hour "stage1.extra_hour_fold_off.wall_ms could not be read"
  on_ms="$(py get "$STAMPS" stage1.extra_hour_fold_on.wall_ms)" || fail_step stage1-extra-hour "stage1.extra_hour_fold_on.wall_ms could not be read"
  code=0
  line="$(py stage1-extra-hour "${out_dir}/stage1-extra-hour-fold-off.json" "${out_dir}/stage1-extra-hour-fold-on.json" \
    "$off_ms" "$on_ms" "$STAGE1_LEAFLESS_HEAD" "$STAGE1_ROUTED_HEAD" "$fold_on_head" \
    "${out_dir}/stage1-extra-hour.inspect.txt" "${out_dir}/stage1-extra-hour.objects.jsonl" \
    "${out_dir}/stage1-routed.inspect.txt")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.extra_hour "failed: ${line:-stage1-extra-hour exited $code}"
    fail_step stage1-extra-hour "${line:-stage1-extra-hour exited $code}"
  fi
  stamp_set stage1.extra_hour "$line"
  stage1_recreate stage1-extra-hour stage1.extra_hour_restart "$STAGE1_ROUTED_FLAGS"
  step_done stage1-extra-hour
}

# stage1_l1_step: ADR-3694 row 6. Once the last ingest hour is sealed,
# ravel-server stopped, `maintain compact-tenant` compacts the tenant's
# sealed metrics hours to L1, a --writers-stopped fold with the label index
# publishes them, the listing stamps the L0 and L1 objects, and the corpus
# runs routed as stage1-l1; <out>/stage1-l1-compare.json compares its hashes
# with stage1-routed's.
stage1_l1_step() {
  local code=0 t0 t1 line
  local cmd="ravel-cli maintain compact-tenant --tenant ${RAVEL_TENANT} --signal metrics --shards ${RAVEL_SHARDS} --max-flush-lifetime 0s"
  stage1_wait_until stage1-l1-compact $(((STAGE1_LAST_INGEST_HOUR + 1) * 3600 + 360)) \
    "compaction takes only hours sealed past the 5 minute clock-skew allowance"
  stage1_stop stage1-l1-compact
  t0="$(now_ms)"
  RAVEL_IMAGE="$ravel_image" compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed maintain compact-tenant \
    --tenant "$RAVEL_TENANT" --signal metrics --shards "$RAVEL_SHARDS" --max-flush-lifetime 0s \
    >"${out_dir}/stage1-l1-compact.txt" 2>"${out_dir}/stage1-l1-compact.log" || code=$?
  t1="$(now_ms)"
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/stage1-l1-compact.log"
    stamp_str stage1.l1_compact "failed: ${cmd} exited $code"
    fail_step stage1-l1-compact "${cmd} exited $code; see ${out_dir}/stage1-l1-compact.log"
  fi
  code=0
  line="$(py stage1-compact "${out_dir}/stage1-l1-compact.txt" "$((t1 - t0))" "$cmd")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.l1_compact "failed: $line"
    fail_step stage1-l1-compact "$line"
  fi
  stamp_set stage1.l1_compact "$line"
  step_done stage1-l1-compact
  stage1_catalog_fold stage1-l1-fold stage1.l1_fold stage1-fold stage1-l1-fold \
    --signal metrics --writers-stopped --key-index-leaves --metrics-label-index
  stage1_recreate stage1-l1 stage1.l1_restart "$STAGE1_ROUTED_FLAGS"
  stage1_list stage1-l1 stage1-l1.objects.jsonl
  code=0
  line="$(py stage1-l1 "${out_dir}/stage1-l1.objects.jsonl")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.l1_objects "failed: $line"
    fail_step stage1-l1 "$line"
  fi
  stamp_set stage1.l1_objects "$line"
  stage1_pass l1
  stage1_note stage1.l1_compare stage1-hash-compare "${out_dir}/stage1-routed/diagnostics.json" \
    "${out_dir}/stage1-l1/diagnostics.json" "${out_dir}/stage1-l1-compare.json" routed l1
  stamp_str stage1.l1_not_measured "ADR-3694 row 6 bounds this run does not check: peak container memory under 8 GiB (no memory sample covers the L1 pass), scan GETs at most the unbatched call's plus one per split run and scan wire bytes at most the unbatched call's (no unbatched reference runs; each L1 record's phases are recorded)"
}

# stage1_erasure_step: ADR-3694 row 8, last, since it removes a subject's
# series from the tenant. A reference pass runs the corpus with every
# selector excluding the subject, and the data objects are listed; the
# erasure is submitted; the pending pass runs the corpus as written and must
# answer as the reference with no label-index prune; a one-off `ravel-server
# --mode maintain` container starts and runs the rewrite until `erase status`
# says COMPLETED, polled from the seal deadline on; a fold, an inspect and a
# second listing then show which hours the rewrite replaced and whether any
# part or leaf from before the erasure is still referenced over one.
stage1_erasure_step() {
  local code=0 line request_id key_file i state="" submit_s deadline_s wait_from_s maintain_s cstate
  local matcher="${STAGE1_ERASURE_LABEL}=${STAGE1_ERASURE_VALUE}" name="metricsbench-stage1-maintain"
  local corpus="${out_dir}/stage1-erasure-reference.corpus.json" status_file="${out_dir}/stage1-erasure-status.txt"
  line="$(py stage1-erasure-corpus "$CORPUS" "$corpus" "$STAGE1_ERASURE_LABEL" "$STAGE1_ERASURE_VALUE")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.erasure_corpus "failed: $line"
    fail_step stage1-erasure-reference "$line"
  fi
  stamp_set stage1.erasure_corpus "$line"
  STAGE1_PASS_CORPUS="$corpus" stage1_pass erasure_reference
  stage1_note stage1.erasure_subject stage1-hash-compare "${out_dir}/stage1-routed/diagnostics.json" \
    "${out_dir}/stage1-erasure-reference/diagnostics.json" "${out_dir}/stage1-erasure-subject.json" \
    routed erasure_reference
  stage1_list stage1-erasure-submit stage1-erasure-reference.objects.jsonl
  request_id="$(python3 -I -c 'import uuid; print(uuid.uuid4())')"
  code=0
  RAVEL_IMAGE="$ravel_image" compose run --rm --no-deps -T ravel-qualify \
    --store s3 --tenant-hash-unkeyed erase submit --tenant "$RAVEL_TENANT" --signal metrics \
    --matcher "$matcher" --request-id "$request_id" --reason "metricsbench stage1 erasure arm" \
    >"${out_dir}/stage1-erasure-submit.txt" 2>"${out_dir}/stage1-erasure-submit.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/stage1-erasure-submit.log"
    stamp_str stage1.erasure_submit "failed: ravel-cli erase submit --matcher ${matcher} exited $code"
    fail_step stage1-erasure-submit "ravel-cli erase submit --matcher ${matcher} exited $code"
  fi
  submit_s="$(date -u +%s)"
  stamp_set stage1.erasure_submit "{\"request_id\": \"${request_id}\", \"signal\": \"metrics\", \"matcher\": \"${STAGE1_ERASURE_LABEL}=\\\"${STAGE1_ERASURE_VALUE}\\\"\", \"submitted_at\": \"$(utc_now)\", \"submitted_unix_s\": ${submit_s}, \"file\": \"stage1-erasure-submit.txt\"}"
  step_done stage1-erasure-submit
  stage1_pass erasure_pending
  stage1_note stage1.erasure_compare stage1-hash-compare "${out_dir}/stage1-erasure-reference/diagnostics.json" \
    "${out_dir}/stage1-erasure-pending/diagnostics.json" "${out_dir}/stage1-erasure-compare.json" \
    erasure_reference erasure_pending

  # mktemp creates the key file 0600; the container runs as the invoking
  # user, as stage1_s3 does, so it can read the file through the mount.
  key_file="$(mktemp "${TMPDIR:-/tmp}/metricsbench-maintain.key.XXXXXX")" ||
    fail_step stage1-erasure-rewrite "cannot create the maintain record key file"
  STAGE1_KEY_FILE="$key_file"
  python3 -I -c 'import secrets; print(secrets.token_hex(32))' >"$key_file" || {
    rm -f "$key_file"
    fail_step stage1-erasure-rewrite "cannot write the maintain record key"
  }
  code=0
  # shellcheck disable=SC2086 # STAGE1_MAINTAIN_FLAGS is a word list
  compose run -d --no-deps --user "$(id -u):$(id -g)" --name "$name" --entrypoint /usr/local/bin/ravel-server \
    -v "${key_file}:/maintain.key:ro" ravel-server \
    $STAGE1_MAINTAIN_FLAGS \
    >"${out_dir}/stage1-erasure-maintain.log" 2>&1 || code=$?
  if [ "$code" -ne 0 ]; then
    rm -f "$key_file"
    show_log "${out_dir}/stage1-erasure-maintain.log"
    stamp_str stage1.erasure_rewrite "failed: the maintain container did not start (docker compose run -d exited $code)"
    fail_step stage1-erasure-rewrite "docker compose run -d ... ravel-server --mode maintain exited $code"
  fi
  maintain_s="$(date -u +%s)"
  stamp_set stage1.erasure_maintain "{\"command\": \"ravel-server ${STAGE1_MAINTAIN_FLAGS}\", \"user\": \"$(id -u):$(id -g)\", \"started_unix_s\": ${maintain_s}, \"note\": \"/maintain.key is a throwaway record key, created 0600, mounted read-only and removed when the step ends; the container starts right after the pending pass and runs through the seal wait; no ravel-server flag turns compaction off, so each maintain tick (services/ravel-server/src/maintain.rs run_tick) also runs retention, compaction, the GC sweep and the catalog-object sweeps over the tenant\"}"

  # The rewrite cannot complete before every hour in the request's scope
  # seals: its end plus max_flush_lifetime plus the clock-skew allowance
  # (crates/ravel-maintain/src/erasure_rewrite.rs ack_open_ingest_hours,
  # seal_margin_ns). bucket_in_scope_at_ack (erasure_rewrite.rs:2747-2756)
  # also puts the next hour in scope when it starts within the clock-skew
  # allowance of the acknowledgement, so the last hour in scope is the one
  # holding submit_s + 300. Reading the clock after the submit returns can
  # only name that hour or a later one.
  deadline_s=$((((submit_s + ERASURE_CLOCK_SKEW_S) / 3600 + 1) * 3600 + ERASURE_MAX_FLUSH_LIFETIME_S + ERASURE_CLOCK_SKEW_S))
  wait_from_s="$(date -u +%s)"
  stage1_wait_until stage1-erasure-rewrite "$deadline_s" \
    "the last hour in the erasure's scope at acknowledgement seals at its end plus max_flush_lifetime plus the clock-skew allowance"
  stamp_set stage1.erasure_seal_wait "{\"submitted_unix_s\": ${submit_s}, \"deadline_unix_s\": ${deadline_s}, \"sleep_s\": $((deadline_s > wait_from_s ? deadline_s - wait_from_s : 0)), \"waited_s\": $(($(date -u +%s) - wait_from_s)), \"basis\": \"deadline = the end of the ingest hour holding submit + clock_skew_allowance (bucket_in_scope_at_ack, crates/ravel-maintain/src/erasure_rewrite.rs:2747-2756) + max_flush_lifetime ${ERASURE_MAX_FLUSH_LIFETIME_S}s (crates/ravel-maintain/src/config.rs DEFAULT_MAX_FLUSH_LIFETIME_NS) + clock_skew_allowance ${ERASURE_CLOCK_SKEW_S}s (DEFAULT_CLOCK_SKEW_ALLOWANCE_NS); the pending pass ran first, then the maintain container started, then METRICSBENCH_SLEEP until the deadline, then the first erase status\"}"

  for ((i = 1; i <= ERASURE_TRIES; i++)); do
    code=0
    RAVEL_IMAGE="$ravel_image" compose run --rm --no-deps -T ravel-qualify \
      --store s3 --tenant-hash-unkeyed erase status --tenant "$RAVEL_TENANT" --signal metrics \
      --request-id "$request_id" >"$status_file" 2>"${out_dir}/stage1-erasure-status.log" || code=$?
    if [ "$code" -eq 0 ] && grep -q "^${request_id}: COMPLETED" "$status_file"; then
      state=COMPLETED
      break
    fi
    code=0
    cstate="$("$DOCKER" inspect --format '{{.State.Status}}' "$name" 2>/dev/null)" || code=$?
    if [ "$code" -ne 0 ] || [ "$cstate" != running ]; then
      "$DOCKER" logs "$name" >"${out_dir}/stage1-erasure-maintain.log" 2>&1 || true
      "$DOCKER" rm -f "$name" >>"${out_dir}/stage1-erasure-maintain.rm.log" 2>&1 || true
      rm -f "$key_file"
      STAGE1_KEY_FILE=""
      stamp_str stage1.erasure_rewrite "failed: the maintain container is ${cstate:-not inspectable (docker inspect exited $code)} at poll ${i}, before erase status said COMPLETED"
      fail_step stage1-erasure-rewrite "the maintain container ${name} is ${cstate:-not inspectable (docker inspect exited $code)} at poll ${i} and erasure ${request_id} is not COMPLETED; see ${out_dir}/stage1-erasure-maintain.log"
    fi
    "$SLEEP" "$ERASURE_INTERVAL"
  done
  "$DOCKER" logs "$name" >"${out_dir}/stage1-erasure-maintain.log" 2>&1 || true
  "$DOCKER" rm -f "$name" >>"${out_dir}/stage1-erasure-maintain.rm.log" 2>&1 || true
  rm -f "$key_file"
  STAGE1_KEY_FILE=""
  if [ "$state" != COMPLETED ]; then
    stamp_str stage1.erasure_rewrite "failed: erase status did not say COMPLETED after ${ERASURE_TRIES} tries ${ERASURE_INTERVAL}s apart"
    fail_step stage1-erasure-rewrite "erasure ${request_id} not COMPLETED after ${ERASURE_TRIES} tries ${ERASURE_INTERVAL}s apart; see ${status_file}"
  fi
  code=0
  line="$(py stage1-erasure-status "$status_file" "$request_id")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.erasure_rewrite "failed: ${line:-stage1-erasure-status exited $code}"
    fail_step stage1-erasure-rewrite "${line:-stage1-erasure-status exited $code}"
  fi
  stamp_set stage1.erasure_rewrite "$line"
  stamp_str stage1.erasure_rewrite.path "a one-off ravel-server --mode maintain container (${name}, --maintain-tenant ${RAVEL_TENANT} --disable-fold), removed once erase status said COMPLETED"
  step_done stage1-erasure-rewrite
  stage1_stop stage1-erasure-fold
  stage1_catalog_fold stage1-erasure-fold stage1.erasure_fold stage1-fold stage1-erasure-fold \
    --signal metrics --writers-stopped --key-index-leaves --metrics-label-index
  stage1_catalog_state erasure
  stage1_list stage1-erasure stage1-erasure.objects.jsonl
  code=0
  line="$(py stage1-erasure-leaves "${out_dir}/stage1-erasure-reference.inspect.txt" \
    "${out_dir}/stage1-erasure.inspect.txt" "${out_dir}/stage1-erasure-reference.objects.jsonl" \
    "${out_dir}/stage1-erasure.objects.jsonl")" || code=$?
  if [ "$code" -ne 0 ]; then
    stamp_str stage1.erasure_leaves "failed: $line"
    fail_step stage1-erasure "$line"
  fi
  stamp_set stage1.erasure_leaves "$line"
  stamp_str stage1.erasure_sweep "not checked: the erasure's physical sweep waits out the protection horizon, which is longer than a run"
  step_done stage1-erasure
}

# stage1_steps: after readback-folded, the Stage 1 steps in their order.
stage1_steps() {
  local k arm
  if [ "$INGEST_FAILED" -eq 1 ] || [ "$FOLDED_RAN" -eq 0 ]; then
    local why="the ingest verification failed, so the run is a diagnostic already"
    [ "$INGEST_FAILED" -eq 1 ] || why="the folded steps were skipped: ${FOLDED_SKIPPED}"
    for k in $STAGE1_KEYS; do
      stamp_str "stage1.${k}" "skipped: $why"
    done
    for arm in $STAGE1_ARMS; do
      if stage1_arm_on "$arm"; then
        for k in $(stage1_arm_keys "$arm"); do
          stamp_str "stage1.${k}" "skipped: $why"
        done
      fi
    done
    say "stage1 steps skipped: $why"
    return 0
  fi
  # The background fold goes off before the run's own fold and stays off: each
  # later recreate keeps --disable-fold.
  stage1_recreate stage1-fold stage1.fold_restart "$STAGE1_UNROUTED_FLAGS"
  stamp_str stage1.background_fold "off from stage1-fold onward: ravel-server was recreated with --disable-fold before stage1-fold, and every later Stage 1 recreate (stage1.unrouted_restart, stage1.restart) passes it too, so only stage1-fold and stage1-leaves change the snapshot the passes read"
  stage1_catalog_fold stage1-fold stage1.fold stage1-fold stage1-fold --signal metrics
  stage1_recreate stage1-unrouted stage1.unrouted_restart "$STAGE1_UNROUTED_FLAGS"
  stage1_pass unrouted
  # The arms that change the tenant (a new hour, L1 rewrites, an erasure) run
  # after every pass that reads the original snapshot.
  [ "$arm_control" -eq 0 ] || stage1_control_step
  # The extra-hour arm times its leaf-off fold from the HEAD no leaf was ever
  # built on, and its leaf-on fold from the routed one; both are kept under
  # <out>/stage1-s3 until that arm ends.
  if [ "$arm_extra_hour" -eq 1 ]; then
    stage1_head_fetch stage1-leaves head-leafless
    STAGE1_LEAFLESS_HEAD="$STAGE1_HEAD_SHA"
  fi
  stage1_catalog_fold stage1-leaves stage1.leaves stage1-leaves stage1-leaves \
    --rebuild-key-index --signal metrics --metrics-label-index
  if [ "$arm_extra_hour" -eq 1 ]; then
    stage1_head_fetch stage1-restart head-routed
    STAGE1_ROUTED_HEAD="$STAGE1_HEAD_SHA"
  fi
  stage1_recreate stage1-restart stage1.restart "$STAGE1_ROUTED_FLAGS"
  step_done stage1-restart
  stage1_pass routed
  stage1_compare
  stage1_holders_step
  STAGE1_LAST_INGEST_HOUR="$INGEST_END_HOUR"
  [ "$arm_extra_hour" -eq 0 ] || stage1_extra_hour_step
  [ "$arm_l1" -eq 0 ] || stage1_l1_step
  [ "$arm_erasure" -eq 0 ] || stage1_erasure_step
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
  stamp_str query_scrapes "not applicable: diagnostic run starts no Ravel or Mimir and runs no query step"
  stamp_str object_spans "not applicable: diagnostic run starts no Ravel"
  stamp_str cold_ravel_diagnostic "not applicable: diagnostic run starts no Ravel"

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
  # Replaced when each capture runs, so a run that ends before one carries
  # why it is missing, never null.
  stamp_str object_spans "not taken: the run ended before the object spans step"
  stamp_str cold_ravel_diagnostic "not taken: the run ended before the cold pass"
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
  local prov_tail=("${store_args[@]}" "${pins[@]}"
    --config "ravel_image=${ravel_image}"
    --config "ravel_image_kind=${ravel_image_kind}"
    ${ravel_image_id:+--config "ravel_image_id=${ravel_image_id}"}
    --config "ravel_source_commit=${ravel_source_commit}"
    --config "harness_commit=$(py get "$STAMPS" harness_commit)")
  local it_env=(env -u RAVEL_INSTANCE_TYPE)
  [ "$host_instance_type" = "unknown" ] || it_env=(env "RAVEL_INSTANCE_TYPE=${host_instance_type}")
  # Only the main report's provenance: the Stage 1 passes carry their own
  # regime and comparable=false.
  local main_prov=()
  [ "$stage1" -eq 0 ] || main_prov=(--config "comparable=false"
    --config "comparability_reason=MB_STAGE1=1: ravel-server ran with ${STAGE1_UP_FLAGS} (not the default command line) through this report's passes")
  # ravel_git_commit names the source of the image under test, not this
  # checkout: GITHUB_SHA wins over git in metricsbench_report.
  code=0
  "${it_env[@]}" GITHUB_SHA="$ravel_source_commit" "${bin_dir}/metricsbench_report" provenance \
    --workload "$WORKLOAD" --corpus "$CORPUS" \
    "${prov_tail[@]}" ${main_prov[@]+"${main_prov[@]}"} \
    >"${out_dir}/provenance.json" 2>>"${out_dir}/provenance.log" || code=$?
  if [ "$code" -ne 0 ]; then
    show_log "${out_dir}/provenance.log"
    fail_step provenance "metricsbench_report provenance exited $code"
  fi
  step_done provenance

  code=0
  http="$(curl -sS --max-time 10 -o "${out_dir}/ravel-metrics-at-query.txt" -w '%{http_code}' \
    "${RAVEL_BASE}/metrics" 2>/dev/null)" || code=$?
  local scrape
  scrape="$(scrape_reason "$code" "$http" "${out_dir}/ravel-metrics-at-query.txt")"
  fold=""
  if [ "$scrape" = ok ]; then
    fold="$(py fold-gauge "${out_dir}/ravel-metrics-at-query.txt")" || fold=""
    stamp_str ravel_catalog_fold "${fold:-absent}"
  else
    stamp_str ravel_catalog_fold "unknown: the /metrics scrape before the query step ${scrape}"
  fi
  stamp_str query_scrapes.ravel_before "$scrape"
  scrape_query_metrics "${MIMIR_BASE}/metrics" "${out_dir}/mimir-metrics-at-query.txt" mimir_before
  stamp_str query_scrapes.ravel_after "not taken: the query step did not complete"
  stamp_str query_scrapes.mimir_after "not taken: the query step did not complete"
  step_done fold-state

  local query_args=(--ingest-report "${out_dir}/ingest.json" --workload "$WORKLOAD" --corpus "$CORPUS"
    --provenance "${out_dir}/provenance.json"
    --engine "name=prometheus,url=${PROM_BASE}"
    --engine "name=ravel,url=${RAVEL_BASE},bearer_env=${RAVEL_TOKEN_ENV}"
    --engine "name=victoriametrics,url=${VM_BASE}/prometheus"
    --engine "name=mimir,url=${MIMIR_BASE}/prometheus"
    --diagnostics "${out_dir}/diagnostics.json"
    --out "${out_dir}/report.json")
  # The ravel_cost records say which fold state they ran against: unknown when
  # the scrape failed, unfolded while ravel-server has published no fold, else
  # the gauge stamped above.
  if [ "$scrape" != ok ]; then
    query_args+=(--pass-label unknown --fold-state "unknown: the /metrics scrape before the query step ${scrape}")
  elif [ -z "$fold" ]; then
    query_args+=(--pass-label unfolded --fold-state "absent: no ravel_catalog_fold_last_success_timestamp_seconds")
  else
    query_args+=(--pass-label background-folded
      --fold-state "ravel_catalog_fold_last_success_timestamp_seconds=${fold}")
  fi
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
  scrape_query_metrics "${RAVEL_BASE}/metrics" "${out_dir}/ravel-metrics-after-query.txt" ravel_after
  scrape_query_metrics "${MIMIR_BASE}/metrics" "${out_dir}/mimir-metrics-after-query.txt" mimir_after
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

  # The memory verdict is written before the cold pass restarts anything, so
  # its restarts and queries never enter the rss bands or final_bytes.
  memory_step --defer
  if [ "$INGEST_FAILED" -eq 1 ]; then
    stamp_str cold_ravel_diagnostic "not applicable: the ingest verification failed, so the run is a diagnostic already"
  else
    cold_ravel_diagnostic
  fi
  folded_readback
  [ "$stage1" -eq 0 ] || stage1_steps
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
