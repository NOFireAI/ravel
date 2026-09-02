#!/usr/bin/env bash
# TLA+ verification harness (ADR-1113 task T1).
#
# Runs TLC over every formal/tla area, or one named area. An "area" is any
# directory under formal/tla that contains a smoke.cfg; its model-check entry
# module is the single MC*.tla file in that directory (naming convention).
#
# Subcommands:
#   smoke        [-a AREA]   fast reachability + safety (budget 300s per cfg)
#   exhaustive   [-a AREA]   full safety + liveness (budget 3600s per cfg)
#   negative     [-a AREA]   run negative/*.cfg, assert the expected violation
#   traceability [-a AREA]   check every traceability.md source ref resolves
#   all          [-a AREA]   smoke + negative + traceability (the CI lane), then exhaustive
#
# Exit codes: 0 pass; 1 a check failed; 2 toolchain missing (no usable Java).
#
# The TLC jar is fetched once into .cache/tla (gitignored) and checksum-pinned;
# a mismatch refuses to run. Java is taken from RAVEL_TLA_JAVA if set, else the
# `java` on PATH; version 17 or newer is required.
set -u

TLA_VERSION="1.7.4"
TLA_JAR_SHA256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TLA_JAR_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_VERSION}/tla2tools.jar"

SMOKE_BUDGET=300
EXHAUSTIVE_BUDGET=3600

REPO_ROOT="$(git rev-parse --show-toplevel)"
FORMAL_DIR="$REPO_ROOT/formal/tla"
CACHE_DIR="$REPO_ROOT/.cache/tla"
LOG_DIR="$CACHE_DIR/logs"
JAR="$CACHE_DIR/tla2tools-${TLA_VERSION}.jar"
LAST_RUN="$CACHE_DIR/last-run.tsv"

RUN_ID=""

die()  { echo "check-tla: $*" >&2; exit 1; }
note() { echo "check-tla: $*" >&2; }

# --- toolchain --------------------------------------------------------------

resolve_java() {
    local java="${RAVEL_TLA_JAVA:-java}"
    command -v "$java" >/dev/null 2>&1 || {
        note "no Java found (set RAVEL_TLA_JAVA or install a JDK 17+)"
        exit 2
    }
    local ver
    ver="$("$java" -version 2>&1 | head -1 | grep -oE '[0-9]+' | head -1)"
    [ -n "$ver" ] || { note "cannot read Java version from '$java'"; exit 2; }
    if [ "$ver" -lt 17 ]; then
        note "Java $ver is too old; TLC needs 17 or newer"
        exit 2
    fi
    JAVA="$java"
}

ensure_jar() {
    mkdir -p "$CACHE_DIR"
    if [ -f "$JAR" ]; then
        local got
        got="$(sha256sum "$JAR" | awk '{print $1}')"
        if [ "$got" = "$TLA_JAR_SHA256" ]; then
            return 0
        fi
        die "cached $JAR has sha256 $got, expected $TLA_JAR_SHA256; refusing to run (delete it to re-fetch)"
    fi
    note "fetching tla2tools $TLA_VERSION"
    local tmp="$JAR.download"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$TLA_JAR_URL" -o "$tmp" || die "download failed from $TLA_JAR_URL"
    elif command -v wget >/dev/null 2>&1; then
        wget -q "$TLA_JAR_URL" -O "$tmp" || die "download failed from $TLA_JAR_URL"
    else
        die "neither curl nor wget available to fetch the TLC jar"
    fi
    local got
    got="$(sha256sum "$tmp" | awk '{print $1}')"
    if [ "$got" != "$TLA_JAR_SHA256" ]; then
        rm -f "$tmp"
        die "downloaded jar has sha256 $got, expected $TLA_JAR_SHA256; refusing to run"
    fi
    mv "$tmp" "$JAR"
}

# --- area discovery ---------------------------------------------------------

discover_areas() {
    local d
    for d in "$FORMAL_DIR"/*/; do
        [ -f "${d}smoke.cfg" ] && basename "$d"
    done
}

area_module() {
    # The model-check entry module: the single MC*.tla in the area directory.
    local area_dir="$1" m
    m="$(ls "$area_dir"/MC*.tla 2>/dev/null | head -1)" || true
    [ -n "$m" ] || die "no MC*.tla entry module in $area_dir"
    basename "$m" .tla
}

# --- TSV --------------------------------------------------------------------

truncate_tsv() {
    mkdir -p "$CACHE_DIR"
    printf 'run-id\tarea\tcfg\tstates\tdistinct\tdepth\tseconds\tresult\n' > "$LAST_RUN"
}

record_row() {
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$RUN_ID" "$1" "$2" "$3" "$4" "$5" "$6" "$7" >> "$LAST_RUN"
}

# --- TLC invocation ---------------------------------------------------------
# Runs TLC on one cfg. Echoes the log path. Returns TLC's exit code.
# CHECK_DEADLOCK FALSE in a cfg means the model has intentional stutter/terminal
# states, so pass -deadlock (TLC's flag that DISABLES the deadlock check).
run_tlc() {
    local area="$1" module="$2" cfg="$3" budget="$4" logfile="$5"
    local area_dir="$FORMAL_DIR/$area"
    local deadlock=""
    if grep -qiE '^[[:space:]]*CHECK_DEADLOCK[[:space:]]+FALSE' "$cfg"; then
        deadlock="-deadlock"
    fi
    local metadir="$CACHE_DIR/meta/$area"
    mkdir -p "$metadir"
    # The wall-clock ceiling needs coreutils timeout (gtimeout from Homebrew
    # coreutils on macOS). Without either the run is unbounded, announced once;
    # CI and the fleet executors are Linux and always have timeout.
    local -a wrap=()
    if [ -z "${TIMEOUT_BIN+x}" ]; then
        if command -v timeout >/dev/null 2>&1; then TIMEOUT_BIN=timeout
        elif command -v gtimeout >/dev/null 2>&1; then TIMEOUT_BIN=gtimeout
        else
            TIMEOUT_BIN=""
            note "neither timeout nor gtimeout on PATH: running TLC without a wall-clock ceiling"
        fi
    fi
    if [ -n "$TIMEOUT_BIN" ]; then wrap=("$TIMEOUT_BIN" "$budget"); fi
    local code=0
    # ${wrap[@]+...}: an empty array is "unbound" under set -u on bash 3.2.
    ( cd "$area_dir" && ${wrap[@]+"${wrap[@]}"} "$JAVA" -XX:+UseParallelGC \
        -DTLA-Library="$area_dir" -cp "$JAR" tlc2.TLC \
        -config "$cfg" -metadir "$metadir" -workers auto $deadlock "$module" ) > "$logfile" 2>&1 || code=$?
    return $code
}

log_field() {
    # log_field <logfile> <regex-with-one-number> -> the number, or "-".
    # TLC prints periodic Progress lines (with thousands-commas) before the
    # final no-comma summary, so strip commas and take the LAST match, not the
    # first, to read the completed total rather than an early progress sample.
    local n
    n="$(tr -d ',' < "$1" 2>/dev/null | grep -oE "$2" | tail -1 | grep -oE '[0-9]+' | tail -1)"
    echo "${n:--}"
}

# --- checks -----------------------------------------------------------------

check_model() {
    # check_model <area> <kind: smoke|exhaustive>
    local area="$1" kind="$2"
    local area_dir="$FORMAL_DIR/$area"
    local cfg="$area_dir/${kind}.cfg"
    [ -f "$cfg" ] || { note "$area: no ${kind}.cfg, skipping"; return 0; }
    local module budget logfile
    module="$(area_module "$area_dir")"
    if [ "$kind" = exhaustive ]; then budget=$EXHAUSTIVE_BUDGET; else budget=$SMOKE_BUDGET; fi
    mkdir -p "$LOG_DIR"
    logfile="$LOG_DIR/${area}-${kind}.log"

    local start=$SECONDS code=0
    run_tlc "$area" "$module" "$cfg" "$budget" "$logfile" || code=$?
    local secs=$((SECONDS - start))

    local states distinct depth
    states="$(log_field "$logfile" '[0-9]+ states generated')"
    distinct="$(log_field "$logfile" '[0-9]+ distinct states found')"
    depth="$(log_field "$logfile" 'search is [0-9]+')"

    if [ "$code" -eq 124 ]; then
        record_row "$area" "${kind}.cfg" "$states" "$distinct" "$depth" "$secs" "TIMEOUT"
        note "$area ${kind}: TIMEOUT after ${budget}s (log: $logfile)"
        return 1
    fi
    if [ "$code" -ne 0 ]; then
        record_row "$area" "${kind}.cfg" "$states" "$distinct" "$depth" "$secs" "FAIL"
        note "$area ${kind}: TLC exit $code (log: $logfile)"
        grep -iE 'is violated|Error:' "$logfile" | head -3 >&2
        return 1
    fi
    record_row "$area" "${kind}.cfg" "$states" "$distinct" "$depth" "$secs" "PASS"
    note "$area ${kind}: PASS  states=$states distinct=$distinct depth=$depth ${secs}s"
    return 0
}

check_negative() {
    local area="$1"
    local area_dir="$FORMAL_DIR/$area"
    local negdir="$area_dir/negative"
    [ -d "$negdir" ] || { note "$area: no negative/ directory, skipping"; return 0; }
    local module logfile rc=0
    module="$(area_module "$area_dir")"
    mkdir -p "$LOG_DIR"

    local cfg
    for cfg in "$negdir"/*.cfg; do
        [ -e "$cfg" ] || continue
        local name expect
        name="$(basename "$cfg" .cfg)"
        expect="$negdir/$name.expect"
        [ -f "$expect" ] || { note "$area negative $name: no .expect file"; rc=1; continue; }

        local want_exit want_prop
        want_exit="$(grep -E '^exit=' "$expect" | head -1 | cut -d= -f2)"
        want_prop="$(grep -E '^property=' "$expect" | head -1 | cut -d= -f2)"
        [ -n "$want_exit" ] && [ -n "$want_prop" ] || {
            note "$area negative $name: .expect must set exit= and property="; rc=1; continue
        }

        logfile="$LOG_DIR/${area}-negative-${name}.log"
        local start=$SECONDS code=0
        run_tlc "$area" "$module" "$cfg" "$SMOKE_BUDGET" "$logfile" || code=$?
        local secs=$((SECONDS - start))
        local states distinct
        states="$(log_field "$logfile" '[0-9]+ states generated')"
        distinct="$(log_field "$logfile" '[0-9]+ distinct states found')"

        # exit 12 = safety (invariant) violation; 13 = temporal (property).
        local viol_re
        if [ "$want_exit" = 13 ]; then
            viol_re="property $want_prop is violated|Temporal properties were violated"
        else
            viol_re="Invariant $want_prop is violated"
        fi

        if [ "$code" = "$want_exit" ] && grep -qE "$viol_re" "$logfile"; then
            record_row "$area" "negative/$name.cfg" "$states" "$distinct" "-" "$secs" "VIOLATED"
            note "$area negative $name: VIOLATED as expected (exit $code, $want_prop)"
        else
            record_row "$area" "negative/$name.cfg" "$states" "$distinct" "-" "$secs" "FAIL"
            note "$area negative $name: expected exit=$want_exit violating $want_prop, got exit=$code"
            grep -iE 'is violated|Error:' "$logfile" | head -3 >&2
            rc=1
        fi
    done
    return $rc
}

check_traceability() {
    local area="$1"
    local area_dir="$FORMAL_DIR/$area"
    local tfile="$area_dir/traceability.md"
    [ -f "$tfile" ] || { note "$area: no traceability.md, skipping"; return 0; }
    local rc=0 count=0

    # Table rows: | requirement | invariant | source-ref |. The source-ref is
    # a repo-relative path, optionally path:Symbol (a symbol grepped in file).
    local line
    while IFS= read -r line; do
        case "$line" in
            \|*) : ;;            # a table row
            *) continue ;;
        esac
        # skip header and separator rows
        case "$line" in
            *---*) continue ;;
            *[Rr]equirement*) continue ;;
        esac
        local ref
        ref="$(echo "$line" | awk -F'|' '{print $4}' | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | tr -d '`')"
        [ -n "$ref" ] || continue

        local path symbol
        path="${ref%%:*}"
        symbol=""
        [ "$ref" != "$path" ] && symbol="${ref#*:}"

        if [ ! -e "$REPO_ROOT/$path" ]; then
            note "$area traceability: missing source '$path'"
            rc=1; continue
        fi
        if [ -n "$symbol" ] && ! grep -qF "$symbol" "$REPO_ROOT/$path"; then
            note "$area traceability: symbol '$symbol' not found in '$path'"
            rc=1; continue
        fi
        count=$((count + 1))
    done < "$tfile"

    if [ "$rc" -eq 0 ]; then
        note "$area traceability: PASS ($count refs resolve)"
    fi
    return $rc
}

# --- dispatch ---------------------------------------------------------------

usage() {
    sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-1}"
}

main() {
    local cmd="${1:-}"; shift || true
    local only_area=""
    while [ $# -gt 0 ]; do
        case "$1" in
            -a) only_area="${2:-}"; shift 2 ;;
            -h|--help) usage 0 ;;
            *) die "unexpected argument: $1" ;;
        esac
    done

    case "$cmd" in
        smoke|exhaustive|negative|traceability|all) : ;;
        ""|-h|--help) usage 0 ;;
        *) die "unknown subcommand: $cmd" ;;
    esac

    resolve_java

    local areas
    if [ -n "$only_area" ]; then
        [ -d "$FORMAL_DIR/$only_area" ] || die "no such area: $only_area"
        areas="$only_area"
    else
        areas="$(discover_areas)"
    fi
    [ -n "$areas" ] || die "no areas found under $FORMAL_DIR (need a smoke.cfg)"

    # Model-check subcommands need the jar; traceability is pure filesystem.
    case "$cmd" in
        traceability) : ;;
        *) ensure_jar ;;
    esac

    RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse 'HEAD^{tree}')"

    local records_tsv=0
    case "$cmd" in smoke|exhaustive|negative|all) records_tsv=1 ;; esac
    [ "$records_tsv" -eq 1 ] && truncate_tsv

    local rc=0 area
    for area in $areas; do
        case "$cmd" in
            smoke)        check_model "$area" smoke || rc=1 ;;
            exhaustive)   check_model "$area" exhaustive || rc=1 ;;
            negative)     check_negative "$area" || rc=1 ;;
            traceability) check_traceability "$area" || rc=1 ;;
            all)
                check_model "$area" smoke || rc=1
                check_negative "$area" || rc=1
                check_traceability "$area" || rc=1
                check_model "$area" exhaustive || rc=1
                ;;
        esac
    done

    if [ "$rc" -eq 0 ]; then note "$cmd: all checks passed"; else note "$cmd: FAILURES (see logs in $LOG_DIR)"; fi
    return $rc
}

main "$@"
