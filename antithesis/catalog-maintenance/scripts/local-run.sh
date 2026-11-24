#!/bin/bash
# The local loop (Antithesis skill, workload/local-loop.md), on the Linux
# build host: start the stack from scratch under the same compose file a run
# uses (libfaketime and the clock jump included), play the driver's first_
# command, let the workload run until the driver's forced eventually ends the
# timeline, probe every clock, then collect every container's SDK output and
# logs and gate on what a run would report as failing.
#
# Usage: scripts/local-run.sh <out-dir> [seconds]
#   seconds   workload time before the driver's eventually runs
#             (WORKLOAD_EVENTUALLY_AFTER_SECS, default 240)
#   WORKLOAD_INJECT_SWAPPED_RECORD=1  the self-check: the model expects one
#             seeded record moved to another write, so the multiset assertion
#             must fail, and nothing else may.
#   WORKLOAD_INJECT_STALE_TOKEN_RESULT=1  the token-read self-check: the
#             driver counts one returned record twice in every token read it
#             judges, so the exactly-once token-read assertion must fail, and
#             nothing else may.
#   RAVEL_ANTITHESIS_MUTANT=<area>/<stem>  a mutant control run (ADR-4326
#             decision 3) of a catalog, resharding or maintenance mutant: the
#             stack runs the config copy scripts/mutant-config.sh writes, over
#             the catalog-maintenance-mutants images, with that mutant
#             selected on every ravel-server container. The property the
#             mutant must make false is the one its own area's
#             formal/tla/<area>/antithesis-mutants.tsv names for it.
#   WORKLOAD_PROFILE=reshard-ownership  the reshard-ownership profile
#             (antithesis/reshard-ownership/README.md): its config, its owned
#             areas and its required assertions on top of this workload's.
#             Default catalog-maintenance (scripts/profile.sh).
#
# Exit 0: every gate passed (or, in the self-check, exactly the injected
# failure appeared; in a mutant run, the mutant's property was evaluated
# false at least once). Exit 1: any gate failed or could not be checked; the
# FAIL lines say which. In a mutant run every other gate, and every other
# always-type assertion that went false, is reported and does not decide the
# exit code.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WL="$(cd "$HERE/.." && pwd)"
OUT="${1:?out dir}"
SECS="${2:-240}"
MUTANT="${RAVEL_ANTITHESIS_MUTANT:-}"
# shellcheck source=profile.sh
. "$HERE/profile.sh" || exit 1
# shellcheck source=mutant-tsv.sh
. "$HERE/mutant-tsv.sh"
COMPOSE_BASE="$PROFILE_DIR/config/docker-compose.yaml"
echo "profile $WORKLOAD_PROFILE: $COMPOSE_BASE"
GATE=FAIL
if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ] && [ "${WORKLOAD_INJECT_STALE_TOKEN_RESULT:-}" = 1 ]; then
    echo "refused: the two self-checks are separate runs; unset one of WORKLOAD_INJECT_SWAPPED_RECORD and WORKLOAD_INJECT_STALE_TOKEN_RESULT"
    exit 1
fi
if [ -n "$MUTANT" ]; then
    if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ] || [ "${WORKLOAD_INJECT_STALE_TOKEN_RESULT:-}" = 1 ]; then
        echo "refused: a self-check and a mutant run are separate runs; unset the WORKLOAD_INJECT_ variable or RAVEL_ANTITHESIS_MUTANT"
        exit 1
    fi
    mkdir -p "$OUT"
    rm -rf "$OUT/mutant-config"
    # Refuses a name that is not one of the mutants mutant-tsv.sh lists.
    "$HERE/mutant-config.sh" "$MUTANT" "$OUT/mutant-config" || exit 1
    COMPOSE_BASE="$OUT/mutant-config/docker-compose.yaml"
    TSV=$(mutant_tsv "$MUTANT") || { echo "refused: no mutant tsv for $MUTANT"; exit 1; }
    expected=$(awk -F '\t' -v m="$MUTANT" '$2 == m { print $3 }' "$TSV")
    case "$expected" in
        '' | *$'\n'*) echo "refused: $TSV names not exactly one property for $MUTANT"; exit 1 ;;
    esac
    # The property each mutant must make false. The corrupt-HEAD mutant
    # switches off both delete paths' HEAD guards, and each path asserts
    # CorruptHeadFailsClosedOnDeletePaths under its own literal, so either
    # one going false is the property going false.
    ACCEPTED=("$expected")
    if [ "$MUTANT" = catalog/corrupt-head-ignores-delete-gate ]; then
        sweep_literal="catalog: a catalog sweep never deletes under an unreadable HEAD"
        sweep_rs="$(cd "$WL/../.." && pwd)/crates/ravel-maintain/src/sweep.rs"
        if ! grep -qF "$sweep_literal" "$sweep_rs"; then
            echo "refused: $sweep_rs does not assert '$sweep_literal'; update the literal here"
            exit 1
        fi
        ACCEPTED+=("$sweep_literal")
    fi
    # A ravel-server built without antithesis-mutants ignores
    # RAVEL_ANTITHESIS_MUTANT, and the run would test no mutant at all.
    features=$(docker image inspect -f '{{ index .Config.Labels "org.ravel.antithesis.features" }}' \
        ravel-antithesis-server:catalog-maintenance-mutants 2>/dev/null) || features=""
    case ",$features," in
        *,antithesis-mutants,*) ;;
        *)
            echo "refused: ravel-antithesis-server:catalog-maintenance-mutants was built with features '$features', not antithesis-mutants; build it with MUTANTS=1 scripts/build.sh"
            exit 1
            ;;
    esac
    GATE="REPORT (mutant run, not gated)"
    echo "mutant $MUTANT: expecting \"$expected\" to be violated"
fi
# The driver's own setup budget is 600 s (SETUP_BUDGET in driver/src/main.rs);
# this bound and launch.sh's --setup-timeout add room for the containers to
# start around it.
SETUP_LIMIT=900
# The eventually waits up to 300 s for recovery, then jumps the clocks, runs
# up to six fold races, five record races, one unfolded-record sweep and one
# unreadable-HEAD sweep (README.md, "The forced races"; about 2200 s between
# them at their waits' bounds), up to three divergent-record and three
# missing-part races (250 s each at their bounds: the 45 s wait for a held
# PUT, the 10 s compaction budget, the 10 s wait for the held result, 2 s,
# the 60 s before the tamper is undone with faults stopped, and 60 s of
# undo retries plus one 60 s attempt), folds, compacts and audits, takes the
# two sweep jumps (each with its reread and serving waits), sweeps, folds,
# audits again and reads; 4800 s and the 6 * 250 s bound all of it.
# The fresh compaction read
# (README.md, "The fresh compaction read") adds 900 s: its compact-bucket
# timeout (300 s), the 120 s wait for the record, six token reads at their
# 30 s timeout with one 2 s retry each (372 s), and its writes, which ack in
# well under a second each once faults stopped. On top of that each of its up
# to 25 clock moves (the jump, the 17 race nudges, three moves of the unfolded
# sweep, one of the unreadable-HEAD sweep, the fresh compaction's nudge, the
# two sweep jumps) first holds HEAD still for 50 s and keeps it frozen through
# one reread (README.md, "HEAD holds still before every move"): 60 s each.
# After the window jump the driver waits up to 150 s more for a server sweep
# to delete an input.
END_LIMIT=$((SECS + 4800 + 6 * 250 + 900 + 25 * 60 + 150))
# The reshard profile's situations run first in the eventually: the
# activation writes, the fence (a 75 s stale-view wait, one move, writes
# that may each wait out a store timeout, and up to 300 s of recovery), and
# up to two decrease cycles of up to three moves each, with their bursts and
# straggler reads. Seven more moves of 60 s, and 1500 s for the rest.
[ "$WORKLOAD_PROFILE" = reshard-ownership ] && END_LIMIT=$((END_LIMIT + 7 * 60 + 1500))
SERVERS="ravel1 ravel2 maint1 maint2"
MULTISET="catalog: compaction preserves the acked multiset"
UPPER="catalog: no scan shows a phantom or an excess copy"
TOKEN_READ="catalog: a token read returns each acked record of its write exactly once"
# Reachable in every plain local run: the clock jump, the sealing it exists
# for, the compaction the sealing enables (ADR-4326 decision 5), and the sweep
# jumps, sweep and deletion of a superseded input the compaction enables.
# Then each forced race (README.md, "The forced races") reaching the
# situation it forces: a server fold losing its HEAD CAS, a server's
# compaction record PUT losing to a concurrent compaction, the clocks
# passing a record's protection horizon and pinned-query window while HEAD
# had not folded it, and HEAD staying unreadable while the clocks passed the
# protection horizon. Each is the situation one silent mutant needs; a run
# that never reaches it cannot tell that mutant from clean code. The driver
# cannot see a server's sweep pass, so the SUT's own "delete pass refused on
# an unreadable HEAD" (both delete paths in ravel-maintain) is required too:
# it proves a server sweep read the undecodable HEAD and stopped. The two
# tampering record races each reach the situation one maintenance mutant
# needs, and the maintenance properties those mutants break must each have
# been evaluated and held: the divergent-input-set one is evaluated only
# where the divergent-record race puts a loser, with its own reachability
# beside it; the winner-part one, on clean code, only where a loser
# converges, which the plain record race gives. Last, the
# token reads (README.md, "Token reads across the lifecycle") at each state:
# after compaction with the input's commit record still there, after HEAD
# named the compaction output, after the sweep deleted the input's commit
# record, twice on one node, and on a node restarted since the write (the
# restart below); and the SUT's own evidence read on the token fallback,
# which runs only on a node that has not cached the gone commit record
# (issue #4490).
REQUIRED=(
    "catalog: the workload starts"
    "clock: the driver's clock jumped forward"
    "catalog: a sealed seeded hour was folded into HEAD"
    "clock: an hour ingested live was sealed by a fold"
    "catalog: a compaction record was published"
    "clock: the clocks jumped past the sweep's protection horizon and pinned-query window"
    "catalog: an on-demand sweep succeeded"
    "catalog: sweep deleted a superseded compaction input"
    "catalog: fold HEAD CAS lost"
    "catalog: a server fold lost its HEAD CAS to a fold one sealed hour ahead"
    "catalog: a server's compaction record PUT lost to a concurrent compaction"
    "catalog: a server's compaction record PUT lost to a record whose input-set hash diverges from its own"
    "catalog: a server's compaction record PUT lost to a record whose part is missing from the store"
    "maintenance: a loser observed a divergent input set and alarmed"
    "maintenance: a loser observing a divergent input-set hash fails closed and never overwrites the record"
    "maintenance: a loser reports Converged only when the winner part is observed present in the store"
    "catalog: the clocks passed a compaction record's protection horizon and pinned-query window while HEAD had not folded it"
    "catalog: HEAD stayed unreadable while the clocks passed the protection horizon"
    "catalog: delete pass refused on an unreadable HEAD"
    "catalog: a token read ran after its input was compacted"
    "catalog: a token read ran after HEAD named the compaction output"
    "catalog: a token read ran after its input commit record was swept"
    "catalog: a token read ran twice on one node"
    "catalog: a token read ran on a node restarted since the write"
    "commit: a commit token's stale-answer evidence read evaluated"
)
# The reshard profile (antithesis/reshard-ownership/README.md, "Required"),
# the pre-registered rows of D6 of the #4340 step 5 decision: the increase's
# generation append won its CAS (in ravel-cli-ant's own file) and a write
# after the jump landed at an added shard; the fence situation was reached
# (each node's grace routing and fence are checked per node below); a
# decrease left a straggler, a read resolved it above the active count, and a
# write after the decrease routed below 4; a forged token was refused and the
# SUT evaluated its durability property; HEAD folded a post-activation hour
# with data at both added shards.
RESHARD_REQUIRED=(
    "resharding: generation append CAS won"
    "reshard: a generation activated"
    "reshard: the clocks passed the writers' lead horizon while the provisioning record's reads were refused"
    "reshard: a write routed before a decrease landed at a retired shard in its activation hour"
    "resharding: a token resolved at a shard index above the active count"
    "reshard: a write routed after the decrease activated was acknowledged"
    "reshard: an acknowledged write sits below the shard count it was routed at"
    "reshard: a forged token for a record never written was refused as unsatisfiable"
    "resharding: a commit token whose record is durable resolves across a reshard"
    "reshard: HEAD's footprint named data of a folded post-activation hour at every shard the increase added"
)
# Hit, and held, in each --mode all node's own SDK file.
RESHARD_PER_NODE=(
    "resharding: a stale writer routed on a grace-extended view"
    "resharding: a writer past the lead horizon was fenced"
    "resharding: grace routing continues only within the lead horizon of the last refresh; past it the writer stops"
)
if [ "$WORKLOAD_PROFILE" = reshard-ownership ]; then
    REQUIRED+=("${RESHARD_REQUIRED[@]}")
fi
mkdir -p "$OUT"
export COMPOSE_FILE="$COMPOSE_BASE:$WL/local/docker-compose.local.yaml"
export COMPOSE_PROJECT_NAME=ravel-antithesis-catalog
export WORKLOAD_EVENTUALLY_AFTER_SECS="$SECS"
failures=()
fail() {
    echo "$GATE: $*"
    failures+=("$*")
}

collect_logs() {
    docker compose logs --no-color > "$OUT/compose.log" 2>&1
    docker compose logs --no-color s3proxy > "$OUT/s3proxy.log" 2>&1
    docker compose ps -a > "$OUT/ps.txt" 2>&1
}

abort() {
    echo "FAIL: $*"
    collect_logs
    docker compose stop > /dev/null 2>&1
    exit 1
}

# One line per container: name, status, exit code.
container_state() {
    docker inspect -f '{{.Name}} {{.State.Status}} {{.State.ExitCode}}' "$1" 2>/dev/null | sed 's#^/##'
}

docker compose down -v > /dev/null 2>&1
# `up` waits for faketime and init to exit 0 (service_completed_successfully)
# and fails if either does not.
docker compose up -d || abort "compose up failed (faketime or init did not exit 0?)"

start=$(date +%s)
until docker compose exec -T driver grep -qs antithesis_setup /tmp/sdk.jsonl; do
    read -r _ st code <<< "$(container_state driver)"
    if [ "${st:-}" != running ]; then
        abort "the driver stopped before setup complete (status ${st:-unknown}, exit ${code:-unknown})"
    fi
    if [ $(($(date +%s) - start)) -gt "$SETUP_LIMIT" ]; then
        abort "setup complete never arrived within ${SETUP_LIMIT}s"
    fi
    sleep 5
done
echo "setup complete after $(($(date +%s) - start))s"
docker compose exec -T driver /opt/antithesis/test/v1/main/first_start_workload \
    || abort "first_start_workload failed"

# The driver ends the timeline itself, exiting 0 after the eventually.
# Meanwhile, when the eventually asks for it (`eventually: local loop:
# restart <url> now`, before its token reads after the sweep), restart the
# node local/docker-compose.local.yaml names and tell the driver, which
# waits for that. A ravel-server keeps every commit record it read in
# memory, and both nodes read every live write's record while its hour was
# unsealed, so only a restarted node GETs a swept record, finds it gone and
# takes the token fallback (README.md, "Token reads across the lifecycle").
# In Antithesis the faults restart nodes; here nothing else does. The SDK
# output file is appended to, so the restart loses none of it.
RESTART_NODE=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' driver \
    | sed -n 's/^WORKLOAD_LOCAL_RESTART_NODE=//p')
case "$RESTART_NODE" in
    ravel1 | ravel2) ;;
    *) abort "WORKLOAD_LOCAL_RESTART_NODE in the driver container is '$RESTART_NODE', not ravel1 or ravel2" ;;
esac
restarted=""
: > "$OUT/node-restart.txt"
start=$(date +%s)
while read -r _ st code <<< "$(container_state driver)" && [ "${st:-}" = running ]; do
    if [ $(($(date +%s) - start)) -gt "$END_LIMIT" ]; then
        fail "the driver did not end the timeline within ${END_LIMIT}s"
        break
    fi
    # Through a file: under pipefail, grep -q closing the pipe early would
    # fail the pipeline on the very line it matched.
    if [ -z "$restarted" ] \
        && docker compose logs --no-color driver > "$OUT/driver-poll.log" 2>/dev/null \
        && grep -q "eventually: local loop: restart http://$RESTART_NODE:[0-9]* now" "$OUT/driver-poll.log"; then
        restarted=1
        if docker restart -t 30 "$RESTART_NODE" > /dev/null \
            && docker compose exec -T driver touch /tmp/local-restart-done; then
            echo "$RESTART_NODE restarted at $(date +%s), when the eventually asked" \
                | tee -a "$OUT/node-restart.txt"
        else
            echo "restarting $RESTART_NODE failed" | tee -a "$OUT/node-restart.txt"
        fi
    fi
    sleep 5
done
grep -q "^$RESTART_NODE restarted at " "$OUT/node-restart.txt" \
    || fail "$RESTART_NODE was not restarted when the eventually asked (see node-restart.txt)"
read -r _ st code <<< "$(container_state driver)"
# Read whatever the driver's state: the clock probe below adds up the moves
# its log reports.
driver_log=1
docker compose logs --no-color driver > "$OUT/driver.log" 2>&1 || driver_log=""
[ -n "$driver_log" ] || fail "could not read the driver's logs"
if [ "${st:-}" = exited ]; then
    if [ "${code:-}" != 0 ]; then
        fail "the driver died after setup (exit ${code:-unknown})"
    elif [ -n "$driver_log" ] && ! grep -q 'eventually: ending the timeline' "$OUT/driver.log"; then
        fail "the driver exited 0 without ending the timeline through the eventually"
    fi
elif [ "${st:-}" != running ]; then
    fail "could not read the driver's state (${st:-nothing})"
fi

# Every container: the setup ones exited 0, the rest still running or exited 0.
: > "$OUT/containers.txt"
for svc in faketime init rustfs s3proxy $SERVERS driver; do
    line=$(container_state "$svc")
    echo "${line:-$svc unreadable}" >> "$OUT/containers.txt"
    read -r _ st code <<< "$line"
    case "${st:-}" in
        running) ;;
        exited) [ "${code:-}" = 0 ] || fail "container $svc exited ${code:-unknown}" ;;
        *) fail "container $svc is ${st:-unreadable}" ;;
    esac
done
for svc in faketime init; do
    grep -q "^$svc exited 0$" "$OUT/containers.txt" || fail "setup container $svc did not exit 0"
done

# The clock probe: each running container's wall clock, against the host's,
# must be ahead by the total offset the three planned jumps (the sealing
# jump, then the sweep's horizon and window jumps) and every move the forced
# races made add up to, less the driver's allowed shortfall (60 s). The
# driver prints each such move as `clock: moved forward by <N> s outside the
# planned jumps (<reason>)`. The offset file must hold that total. rustfs is
# read twice, by `date` in its container and by the Last-Modified the rustfs
# process stamps on a PUT. A reading that is empty or not a number is a probe
# failure.
driver_env=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' driver)
jump=0
for var in CLOCK_JUMP_SECS CLOCK_SWEEP_HORIZON_JUMP_SECS CLOCK_SWEEP_WINDOW_JUMP_SECS; do
    secs=$(printf '%s\n' "$driver_env" | sed -n "s/^$var=//p")
    case "$secs" in
        '' | *[!0-9]*) fail "could not read $var from the driver container ('$secs')" ;;
        *) jump=$((jump + secs)) ;;
    esac
done
nudges=0
if [ -n "$driver_log" ]; then
    while IFS= read -r secs; do
        nudges=$((nudges + 1))
        jump=$((jump + secs))
    done < <(sed -n 's/.*clock: moved forward by \([0-9][0-9]*\) s outside the planned jumps (.*/\1/p' "$OUT/driver.log")
fi
offset=$(docker compose exec -T ravel1 cat /faketime/offset 2>/dev/null | tr -d '[:space:]')
[ "$offset" = "+$jump" ] \
    || fail "the offset file holds '$offset', not the +${jump} the three jumps and $nudges forced-race moves add up to"
# Every clock move held HEAD still for the quiet span first and kept it still
# while it landed (README.md, "HEAD holds still before every move"): one
# `clock: jumped` line per move, each with head_quiet_s at least
# head_quiet_needed_s and head_moved_while_landing false: true is a move that
# saw HEAD change while it landed, and null one whose landing read could not
# tell.
if [ -n "$driver_log" ]; then
    sed -n 's/.*clock: jumped (\(.*\))$/\1/p' "$OUT/driver.log" > "$OUT/clock-moves.jsonl"
    moves=$(wc -l < "$OUT/clock-moves.jsonl")
    [ "$moves" -eq $((3 + nudges)) ] \
        || fail "the driver logged $moves clock moves, not the three jumps and $nudges forced-race moves"
    if short=$(jq -r 'select((.head_quiet_s // -1) < (.head_quiet_needed_s // 1e9)
            or .head_moved_while_landing != false) | .reason' "$OUT/clock-moves.jsonl"); then
        [ -z "$short" ] \
            || fail "clock moves that did not hold HEAD still for the quiet span: $(tr '\n' ' ' <<< "$short")"
    else
        fail "could not parse the clock moves in clock-moves.jsonl"
    fi
fi
host_now=$(date +%s)
{
    for svc in rustfs $SERVERS; do
        echo "$svc $(docker compose exec -T "$svc" date +%s 2>/dev/null)"
    done
    # shellcheck disable=SC2016 # expanded inside the container
    echo "rustfs-last-modified $(docker compose exec -T ravel1 sh -c '
        sig="aws:amz:$RAVEL_S3_REGION:s3"; creds="$RAVEL_S3_ACCESS_KEY:$RAVEL_S3_SECRET_KEY"
        url="$RAVEL_S3_ENDPOINT/$RAVEL_S3_BUCKET/antithesis/catalog-maintenance/clock-probe"
        printf x | curl -s --max-time 10 -o /dev/null -X PUT --data-binary @- --aws-sigv4 "$sig" --user "$creds" "$url"
        lm=$(curl -s --max-time 10 -I --aws-sigv4 "$sig" --user "$creds" "$url" | tr -d "\r" | sed -n "s/^[Ll]ast-[Mm]odified: //p")
        [ -n "$lm" ] && env -u LD_PRELOAD date -d "$lm" +%s' 2>/dev/null)"
} > "$OUT/clock-probe.txt"
echo "host $host_now jump $jump" >> "$OUT/clock-probe.txt"
cat "$OUT/clock-probe.txt"
while read -r name reading; do
    case "$name" in host) continue ;; esac
    case "$reading" in
        '' | *[!0-9]*) fail "clock probe of $name read '${reading}'" ;;
        *) [ $((reading - host_now)) -ge $((jump - 60)) ] \
            || fail "clock of $name is $((reading - host_now))s ahead of the host, not the ${jump}s jump" ;;
    esac
done < "$OUT/clock-probe.txt"

# Debug tools in each image.
{
    docker compose exec -T ravel1 sh -c 'curl --version | head -1; jq --version; ravel-cli --help | head -1'
} > "$OUT/debug-tools.txt" 2>&1

# Graceful shutdown: a clean stop exits 0, a kill after the timeout 137.
for svc in ravel2 maint2; do
    docker stop -t 30 "$svc" > /dev/null
    docker inspect -f "$svc exit code after docker stop: {{.State.ExitCode}}" "$svc" >> "$OUT/shutdown.txt"
done

: > "$OUT/missing-sdk.txt"
for svc in driver $SERVERS; do
    docker compose cp "$svc:/tmp/sdk.jsonl" "$OUT/sdk-$svc.jsonl" > /dev/null 2>&1 \
        || echo "no sdk output from $svc" >> "$OUT/missing-sdk.txt"
done
# ravel-cli-ant writes its own SDK file in the driver container
# (driver/src/cli.rs), and only when the driver runs it, so its absence is no
# failure. It joins the sdk-*.jsonl globs below, never driver-assertions.txt:
# its catalog declares every area's assertions, not the driver's.
rm -f "$OUT/sdk-cli.jsonl"
if docker compose cp driver:/tmp/sdk-cli.jsonl "$OUT/sdk-cli.jsonl" > /dev/null 2>&1; then
    echo "sdk-cli.jsonl: collected from ravel-cli-ant"
else
    rm -f "$OUT/sdk-cli.jsonl"
    echo "sdk-cli.jsonl: none, the driver container holds no /tmp/sdk-cli.jsonl"
fi
collect_logs
docker compose stop > /dev/null 2>&1
[ -s "$OUT/missing-sdk.txt" ] && fail "SDK output missing: $(tr '\n' ' ' < "$OUT/missing-sdk.txt")"
# The proxy never ran out of descriptors (issue #4633).
if proxy_fds=$("$HERE/proxy-log-check.sh" "$OUT/s3proxy.log"); then
    echo "$proxy_fds"
else
    fail "$proxy_fds"
fi

# Every assertion with its hit and pass counts, then the ones that would fail.
if ! jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | "\(.[0].display_type)\t\([.[] | select(.hit)] | length)\t\([.[] | select(.hit and .condition)] | length)\t\(.[0].message)"' \
    "$OUT"/sdk-*.jsonl > "$OUT/assertions.unsorted"; then
    abort "could not parse the SDK output"
fi
sort -t "$(printf '\t')" -k4 "$OUT/assertions.unsorted" > "$OUT/assertions.tsv"
rm -f "$OUT/assertions.unsorted"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(if .[0].assert_type == "sometimes" then any(.hit and .condition) | not
             else any(.hit and (.condition | not)) or (.[0].must_hit and (any(.hit) | not)) end)
    | "\(.[0].display_type)\t\(.[0].message)"' "$OUT"/sdk-*.jsonl | sort > "$OUT/would-fail.tsv"
echo "assertions: $(wc -l < "$OUT/assertions.tsv"), would fail: $(wc -l < "$OUT/would-fail.tsv")"
cat "$OUT/would-fail.tsv"
[ -s "$OUT/assertions.tsv" ] || abort "no assertions collected"

# Always-type assertions hit with a false condition, and those a run must hit
# (must_hit, as Antithesis counts them) that were never hit.
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type != "sometimes" and any(.hit and (.condition | not)))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/violated.tsv"
jq -rs '[.[].antithesis_assert // empty] | group_by(.message)[]
    | select(.[0].assert_type == "always" and .[0].must_hit and (any(.hit) | not))
    | .[0].message' "$OUT"/sdk-*.jsonl | sort > "$OUT/never-hit.tsv"
# The ravel-server binary carries every area's assertions, and this workload
# drives only the areas it owns (ADR-4326, per-workload amendment): the areas
# whose mutants the catalog .tsv names, as the literal prefix `<area>: `, and
# every assertion of this workload's own driver (the `clock: ` ones among
# them). An unreached always-assertion of another area is listed, never
# failed.
owned_tsvs=()
for area in $PROFILE_OWNED_TSV; do
    owned_tsvs+=("$(cd "$WL/../.." && pwd)/formal/tla/$area/antithesis-mutants.tsv")
done
owned_areas=$(awk -F '\t' '!/^#/ && $2 ~ /^[a-z0-9-]+\// { sub(/\/.*/, "", $2); print $2 }' "${owned_tsvs[@]}" | sort -u)
[ -n "$owned_areas" ] || abort "could not read the owned areas from ${owned_tsvs[*]}"
jq -r '.antithesis_assert.message // empty' "$OUT/sdk-driver.jsonl" | sort -u > "$OUT/driver-assertions.txt" \
    || abort "could not read the driver's assertions"
[ -s "$OUT/driver-assertions.txt" ] || abort "the driver declared no assertions"
: > "$OUT/never-hit-owned.tsv"
: > "$OUT/never-hit-other-areas.tsv"
: > "$OUT/never-hit-pending.tsv"
while IFS= read -r m; do
    if [ -n "$PROFILE_PENDING" ] && printf '%s\n' "$PROFILE_PENDING" | grep -qxF -- "$m"; then
        echo "$m" >> "$OUT/never-hit-pending.tsv"
        continue
    fi
    mine=""
    for area in $owned_areas; do
        case "$m" in "$area: "*) mine=1 ;; esac
    done
    grep -qxF -- "$m" "$OUT/driver-assertions.txt" && mine=1
    if [ -n "$mine" ]; then
        echo "$m" >> "$OUT/never-hit-owned.tsv"
    else
        echo "$m" >> "$OUT/never-hit-other-areas.tsv"
    fi
done < "$OUT/never-hit.tsv"
while IFS= read -r m; do fail "always-type assertion never hit: $m"; done < "$OUT/never-hit-owned.tsv"
if [ -s "$OUT/never-hit-other-areas.tsv" ]; then
    echo "not this workload's (owned: $(tr '\n' ' ' <<< "$owned_areas")and the driver's own), unreached and not gated:"
    sed 's/^/  /' "$OUT/never-hit-other-areas.tsv"
fi
if [ -s "$OUT/never-hit-pending.tsv" ]; then
    echo "owned, unreached, and pending a forced situation not built yet (scripts/profile.sh), not gated:"
    sed 's/^/  /' "$OUT/never-hit-pending.tsv"
fi
for m in "${REQUIRED[@]}"; do
    hits=$(awk -F '\t' -v m="$m" '$4 == m { print $3 }' "$OUT/assertions.tsv")
    [ -n "$hits" ] && [ "$hits" -gt 0 ] || fail "required assertion never held: $m"
done
if [ "$WORKLOAD_PROFILE" = reshard-ownership ]; then
    # The fence on each node, from that node's own file: each node held a
    # view, routed on it past the refresh interval, and stopped past the
    # lead horizon.
    for node in ravel1 ravel2; do
        for m in "${RESHARD_PER_NODE[@]}"; do
            held=$(jq -r --arg m "$m" 'select(.antithesis_assert.message == $m
                    and .antithesis_assert.hit and .antithesis_assert.condition) | 1' \
                "$OUT/sdk-$node.jsonl" 2>/dev/null | wc -l)
            [ "$held" -gt 0 ] || fail "required assertion never held on $node: $m"
        done
    done
    # Every node served again within 300 s of the fence's thaw-read.
    thawed=$(jq -r 'select(.antithesis_assert.message == "catalog: the system recovers once faults stop"
            and .antithesis_assert.hit and .antithesis_assert.condition
            and .antithesis_assert.details.after == "thaw-read") | 1' "$OUT/sdk-driver.jsonl" | wc -l)
    [ "$thawed" -gt 0 ] || fail "required assertion never held after the thaw-read: catalog: the system recovers once faults stop"
fi

if [ -n "$MUTANT" ]; then
    # The mutant's property decides; every other violation is reported.
    hit=""
    while IFS= read -r m; do
        mine=""
        for p in "${ACCEPTED[@]}"; do
            [ "$m" = "$p" ] && mine=1
        done
        if [ -n "$mine" ]; then
            hit="${hit:-$m}"
        else
            echo "$GATE: always-type assertion violated besides the mutant's: $m"
        fi
    done < "$OUT/violated.tsv"
    [ "${#failures[@]}" -gt 0 ] && echo "${#failures[@]} other gate(s) failed in this mutant run, reported above; see $OUT"
    if [ -n "$hit" ]; then
        echo "mutant OK: $MUTANT violated \"$hit\""
        exit 0
    fi
    echo "FAIL: mutant $MUTANT ran, but none of these was violated: $(printf '"%s" ' "${ACCEPTED[@]}")"
    exit 1
fi

if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ]; then
    # The self-check: the swapped expectation must make the multiset fail,
    # and may also break the upper bound it implies; any other violation is
    # a real one.
    grep -qxF "$MULTISET" "$OUT/violated.tsv" \
        || fail "self-check run, but the multiset assertion did not fail"
    while IFS= read -r m; do
        [ "$m" = "$MULTISET" ] || [ "$m" = "$UPPER" ] || fail "always-type assertion violated besides the injected one: $m"
    done < "$OUT/violated.tsv"
elif [ "${WORKLOAD_INJECT_STALE_TOKEN_RESULT:-}" = 1 ]; then
    # The token-read self-check: the duplicated record must make the
    # exactly-once token-read assertion fail, and nothing else.
    grep -qxF "$TOKEN_READ" "$OUT/violated.tsv" \
        || fail "token-read self-check run, but the token-read assertion did not fail"
    while IFS= read -r m; do
        [ "$m" = "$TOKEN_READ" ] || fail "always-type assertion violated besides the injected one: $m"
    done < "$OUT/violated.tsv"
else
    while IFS= read -r m; do fail "always-type assertion violated: $m"; done < "$OUT/violated.tsv"
fi

if [ "${#failures[@]}" -gt 0 ]; then
    echo "${#failures[@]} gate(s) failed; see $OUT"
    exit 1
fi
if [ "${WORKLOAD_INJECT_SWAPPED_RECORD:-}" = 1 ]; then
    echo "self-check OK: the multiset assertion failed as injected, and nothing else did"
elif [ "${WORKLOAD_INJECT_STALE_TOKEN_RESULT:-}" = 1 ]; then
    echo "self-check OK: the token-read assertion failed as injected, and nothing else did"
else
    echo "OK: no always-type assertion violated or unhit, every required assertion held, every container healthy, every clock jumped"
fi
