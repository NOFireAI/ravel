#!/usr/bin/env bash
# TLA+ verification harness (ADR-1113 task T1).
#
# Runs TLC over every formal/tla area, or one named area. An "area" is any
# directory under formal/tla that carries a smoke config, either a bare
# smoke.cfg or a per-module MC<Spec>.smoke.cfg. Each MC*.tla in the area is a
# model-check entry module; an area may hold more than one.
#
# Subcommands:
#   smoke        [-a AREA]   fast reachability + safety (budget 300s per cfg)
#   exhaustive   [-a AREA]   full safety + liveness (budget 3600s per cfg)
#   negative     [-a AREA]   run negative/*.cfg, assert the expected violation
#   traceability [-a AREA]   check every traceability.md source ref resolves
#   ci           [-a AREA]   smoke + negative + traceability under one run id
#   all          [-a AREA]   ci, then exhaustive, under one run id
#
# Exit codes: 0 pass; 1 a check failed; 2 toolchain missing (no usable Java).
#
# The TLC jar is resolved once and checksum-pinned. Set RAVEL_TLA_TOOLS_JAR to
# an operator-supplied jar (verified against the pin, never downloaded); else it
# is fetched once into .cache/tla (gitignored). A checksum mismatch refuses to
# run. Java is taken from RAVEL_TLA_JAVA if set, else the `java` on PATH;
# version 17 or newer is required.
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
    # Read the line that actually carries the version, not line 1: a
    # JAVA_TOOL_OPTIONS "Picked up ..." banner is printed before it.
    local ver
    ver="$("$java" -version 2>&1 | grep -iE 'version' | head -1 | grep -oE '[0-9]+' | head -1)"
    [ -n "$ver" ] || { note "cannot read Java version from '$java'"; exit 2; }
    if [ "$ver" -lt 17 ]; then
        note "Java $ver is too old; TLC needs 17 or newer"
        exit 2
    fi
    JAVA="$java"
    note "java: $java (version $ver)"
}

ensure_jar() {
    mkdir -p "$CACHE_DIR"
    # RAVEL_TLA_TOOLS_JAR (ADR-1113 D9): an operator-supplied jar. Use it as-is,
    # verify its sha256 against the pin, and refuse on mismatch. Never download
    # in this mode: an air-gapped or reproducible build supplies the jar and a
    # silent fetch would defeat the point.
    if [ -n "${RAVEL_TLA_TOOLS_JAR:-}" ]; then
        [ -f "$RAVEL_TLA_TOOLS_JAR" ] \
            || die "RAVEL_TLA_TOOLS_JAR=$RAVEL_TLA_TOOLS_JAR does not exist"
        local supplied
        supplied="$(sha256sum "$RAVEL_TLA_TOOLS_JAR" | awk '{print $1}')"
        [ "$supplied" = "$TLA_JAR_SHA256" ] \
            || die "RAVEL_TLA_TOOLS_JAR sha256 $supplied != expected $TLA_JAR_SHA256; refusing to run (not downloading)"
        JAR="$RAVEL_TLA_TOOLS_JAR"
        return 0
    fi
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
    # An area is a directory under formal/tla holding at least one smoke config,
    # either the bare smoke.cfg (single-spec area) or a per-module MC*.smoke.cfg.
    local d
    for d in "$FORMAL_DIR"/*/; do
        if [ -f "${d}smoke.cfg" ] || ls "${d}"MC*.smoke.cfg >/dev/null 2>&1; then
            basename "$d"
        fi
    done
}

area_modules() {
    # Every model-check entry module in the area: each MC*.tla, one per line.
    # An area may hold several (e.g. maintenance). Per-module cfg files
    # MC<Spec>.smoke.cfg / MC<Spec>.<kind>.cfg are NOT modules and are excluded.
    local area_dir="$1" m base
    local found=0
    for m in "$area_dir"/MC*.tla; do
        [ -e "$m" ] || continue
        base="$(basename "$m" .tla)"
        echo "$base"
        found=1
    done
    [ "$found" = 1 ] || die "no MC*.tla entry module in $area_dir"
}

module_cfg() {
    # The cfg for one module and kind: prefer the per-module MC<Spec>.<kind>.cfg,
    # else fall back to the bare <kind>.cfg (valid only for a single-spec area).
    # Prints the cfg path, or nothing if neither exists.
    local area_dir="$1" module="$2" kind="$3"
    if [ -f "$area_dir/$module.$kind.cfg" ]; then
        echo "$area_dir/$module.$kind.cfg"
    elif [ -f "$area_dir/$kind.cfg" ]; then
        echo "$area_dir/$kind.cfg"
    fi
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
    # The library path carries the shared common/ module first, then the area
    # dir, so any area can EXTEND or INSTANCE RavelObjectStore. The area dir
    # comes second so a same-named module in the area still wins locally.
    local libpath="$FORMAL_DIR/common:$area_dir"
    local code=0
    # ${wrap[@]+...}: an empty array is "unbound" under set -u on bash 3.2.
    ( cd "$area_dir" && ${wrap[@]+"${wrap[@]}"} "$JAVA" -XX:+UseParallelGC \
        -DTLA-Library="$libpath" -cp "$JAR" tlc2.TLC \
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

# check_bands <area> <cfg-name> <distinct> <depth>
# Compares a PASS run's figures against the optional bands.tsv row for this cfg.
# A missing bands.tsv, or a bands.tsv with no row for this cfg, is not an error
# (bands are opt-in). A row that exists is enforced: the figure must be present
# (not "-") and inside [min,max]. Returns non-zero on any violation.
check_bands() {
    local area="$1" cfg_name="$2" distinct="$3" depth="$4"
    local bands="$FORMAL_DIR/$area/bands.tsv"
    [ -f "$bands" ] || return 0
    local row
    row="$(awk -F'\t' -v c="$cfg_name" '$1==c {print; found=1} END{}' "$bands")"
    [ -n "$row" ] || return 0
    local n
    n="$(awk -F'\t' -v c="$cfg_name" '$1==c' "$bands" | wc -l | tr -d ' ')"
    if [ "$n" != 1 ]; then
        note "$area bands: cfg '$cfg_name' appears $n times in bands.tsv (want exactly 1)"
        return 1
    fi
    local mind maxd mindepth maxdepth
    mind="$(echo "$row" | cut -f2)"
    maxd="$(echo "$row" | cut -f3)"
    mindepth="$(echo "$row" | cut -f4)"
    maxdepth="$(echo "$row" | cut -f5)"
    local f
    for f in "$mind" "$maxd" "$mindepth" "$maxdepth"; do
        case "$f" in
            ''|*[!0-9]*)
                note "$area bands: malformed row for $cfg_name (need cfg, min_distinct, max_distinct, min_depth, max_depth as integers): '$row'"
                return 1 ;;
        esac
    done
    local rc=0
    case "$distinct" in
        ''|*[!0-9]*)
            note "$area bands: $cfg_name distinct figure missing or non-numeric ('$distinct')"; rc=1 ;;
        *)
            if [ "$distinct" -lt "$mind" ] || [ "$distinct" -gt "$maxd" ]; then
                note "$area bands: $cfg_name distinct=$distinct outside [$mind,$maxd]"; rc=1
            fi ;;
    esac
    case "$depth" in
        ''|*[!0-9]*)
            note "$area bands: $cfg_name depth figure missing or non-numeric ('$depth')"; rc=1 ;;
        *)
            if [ "$depth" -lt "$mindepth" ] || [ "$depth" -gt "$maxdepth" ]; then
                note "$area bands: $cfg_name depth=$depth outside [$mindepth,$maxdepth]"; rc=1
            fi ;;
    esac
    return $rc
}

# check_one_model <area> <module> <kind> <cfg>
check_one_model() {
    local area="$1" module="$2" kind="$3" cfg="$4"
    local area_dir="$FORMAL_DIR/$area"
    local cfg_name budget logfile
    cfg_name="$(basename "$cfg")"
    if [ "$kind" = exhaustive ]; then budget=$EXHAUSTIVE_BUDGET; else budget=$SMOKE_BUDGET; fi
    mkdir -p "$LOG_DIR"
    logfile="$LOG_DIR/${area}-${module}-${kind}.log"
    local label="$area/$module ${kind}"

    local start=$SECONDS code=0
    run_tlc "$area" "$module" "$cfg" "$budget" "$logfile" || code=$?
    local secs=$((SECONDS - start))

    local states distinct depth
    states="$(log_field "$logfile" '[0-9]+ states generated')"
    distinct="$(log_field "$logfile" '[0-9]+ distinct states found')"
    depth="$(log_field "$logfile" 'search is [0-9]+')"

    if [ "$code" -eq 124 ]; then
        record_row "$area" "$cfg_name" "$states" "$distinct" "$depth" "$secs" "TIMEOUT"
        note "$label: TIMEOUT after ${budget}s (log: $logfile)"
        return 1
    fi
    if [ "$code" -ne 0 ]; then
        record_row "$area" "$cfg_name" "$states" "$distinct" "$depth" "$secs" "FAIL"
        note "$label: TLC exit $code (log: $logfile)"
        grep -iE 'is violated|Error:' "$logfile" | head -3 >&2
        return 1
    fi
    if ! check_bands "$area" "$cfg_name" "$distinct" "$depth"; then
        record_row "$area" "$cfg_name" "$states" "$distinct" "$depth" "$secs" "BAND"
        note "$label: figures outside declared band (log: $logfile)"
        return 1
    fi
    record_row "$area" "$cfg_name" "$states" "$distinct" "$depth" "$secs" "PASS"
    note "$label: PASS  states=$states distinct=$distinct depth=$depth ${secs}s"
    return 0
}

check_model() {
    # check_model <area> <kind: smoke|exhaustive>
    # Runs every MC*.tla module in the area against its cfg for this kind.
    local area="$1" kind="$2"
    local area_dir="$FORMAL_DIR/$area"
    local rc=0 module cfg modules
    # Collect the modules in the parent shell: a die inside a process
    # substitution would exit only the subshell and leave the loop with
    # nothing to check and rc=0 (an area with no entry module must fail).
    modules="$(area_modules "$area_dir")" || return 1
    while IFS= read -r module; do
        [ -n "$module" ] || continue
        cfg="$(module_cfg "$area_dir" "$module" "$kind")"
        if [ -z "$cfg" ]; then
            if [ "$kind" = smoke ]; then
                note "$area/$module: no ${kind}.cfg or ${module}.${kind}.cfg"; rc=1
            else
                note "$area/$module: no ${kind} cfg, skipping"
            fi
            continue
        fi
        check_one_model "$area" "$module" "$kind" "$cfg" || rc=1
    done <<< "$modules"
    return $rc
}

# negative_module <area-dir> <cfg>
# The module a negative cfg drives: its first line may name one with the
# `\* module: MC<Spec>` convention. Otherwise the area must hold exactly one
# MC*.tla and that module is used.
negative_module() {
    local area_dir="$1" cfg="$2"
    local first named
    first="$(head -1 "$cfg")"
    named="$(printf '%s\n' "$first" | grep -oE 'module:[[:space:]]*MC[A-Za-z0-9_]+' | head -1 | sed 's/.*module:[[:space:]]*//')"
    if [ -n "$named" ]; then
        echo "$named"; return 0
    fi
    local mods count
    mods="$(area_modules "$area_dir")"
    count="$(printf '%s\n' "$mods" | grep -c . )"
    if [ "$count" = 1 ]; then
        printf '%s\n' "$mods"; return 0
    fi
    die "negative $(basename "$cfg"): area has $count MC modules; first line must name one with '\\* module: MC<Spec>'"
}

# liveness_single_property_cfg <src-cfg> <property> -> path of a generated cfg
# For an exit=13 (temporal) negative, TLC 1.7.4 prints only "Temporal
# properties were violated." with no name, so a bare property= check is
# vacuous. Generate a cfg that declares EXACTLY the expected property (every
# other PROPERTY line stripped, the expected one appended): then a violation
# can only be that property, and a wrong property= name makes TLC fail to
# resolve the operator instead of reporting exit 13.
liveness_single_property_cfg() {
    local src="$1" prop="$2"
    local gen="$CACHE_DIR/negcfg"
    mkdir -p "$gen"
    local base out
    base="$(basename "$src" .cfg)"
    out="$gen/$base.$prop.cfg"
    grep -vE '^[[:space:]]*PROPERTY[[:space:]]' "$src" > "$out"
    printf 'PROPERTY %s\n' "$prop" >> "$out"
    echo "$out"
}

check_negative() {
    local area="$1"
    local area_dir="$FORMAL_DIR/$area"
    local negdir="$area_dir/negative"
    [ -d "$negdir" ] || { note "$area: no negative/ directory, skipping"; return 0; }
    local logfile rc=0
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

        local module run_cfg viol_re
        module="$(negative_module "$area_dir" "$cfg")" || { rc=1; continue; }
        if [ "$want_exit" = 13 ]; then
            # Temporal: run a generated cfg declaring only the expected property.
            run_cfg="$(liveness_single_property_cfg "$cfg" "$want_prop")"
            viol_re="Temporal properties were violated"
        else
            run_cfg="$cfg"
            viol_re="Invariant $want_prop is violated"
        fi

        logfile="$LOG_DIR/${area}-negative-${name}.log"
        local start=$SECONDS code=0
        run_tlc "$area" "$module" "$run_cfg" "$SMOKE_BUDGET" "$logfile" || code=$?
        local secs=$((SECONDS - start))
        local states distinct
        states="$(log_field "$logfile" '[0-9]+ states generated')"
        distinct="$(log_field "$logfile" '[0-9]+ distinct states found')"

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

# Resolve one Rust reference of the form crates/<path>.rs::Sym1::Sym2...
# The path must exist and end in .rs (a .tla reference is rejected: a
# traceability row cites the implementation, not the model), and every
# ::-separated symbol must appear in the file. Sets rc=1 in the caller's scope
# via echo of a diagnostic; returns non-zero on failure.
resolve_rust_ref() {
    local area="$1" ref="$2"
    local ref_path="${ref%%::*}"
    case "$ref_path" in
        *.tla) note "$area traceability: ref '$ref' points at a .tla file, not Rust source"; return 1 ;;
        *.rs) : ;;
        *) note "$area traceability: ref '$ref' is not a Rust (.rs) path"; return 1 ;;
    esac
    if [ ! -e "$REPO_ROOT/$ref_path" ]; then
        note "$area traceability: missing source '$ref_path'"; return 1
    fi
    # Walk each ::-separated symbol after the path.
    local rest="${ref#"$ref_path"}"
    rest="${rest#::}"
    local sym
    while [ -n "$rest" ]; do
        sym="${rest%%::*}"
        if [ -n "$sym" ] && ! grep -qF "$sym" "$REPO_ROOT/$ref_path"; then
            note "$area traceability: symbol '$sym' not found in '$ref_path'"; return 1
        fi
        if [ "$rest" = "$sym" ]; then rest=""; else rest="${rest#*::}"; fi
    done
    return 0
}

check_traceability() {
    local area="$1"
    local area_dir="$FORMAL_DIR/$area"
    local tfile="$area_dir/traceability.md"
    [ -f "$tfile" ] || { note "$area: no traceability.md, skipping"; return 0; }
    local rc=0 count=0

    # Five D8 columns:
    #   | TLA+ action or property | meaning | Rust path and symbol
    #   | existing test | new test needed |
    # The "Rust path and symbol" column is required and resolved. The other
    # columns may also carry Rust references (an existing test, or a symbol
    # named in the "new test needed" note); any token that looks like a
    # crates/... reference is resolved too, so a stale test path is caught.
    local line
    while IFS= read -r line; do
        case "$line" in
            \|*) : ;;            # a table row
            *) continue ;;
        esac
        case "$line" in
            *---*) continue ;;
            *[Aa]ction*[Pp]roperty*) continue ;;   # header row
        esac
        # The required source ref is column 3 (awk field 4 after the leading |).
        local src
        src="$(echo "$line" | awk -F'|' '{print $4}' | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | tr -d '`')"
        if [ -z "$src" ]; then
            note "$area traceability: row has no Rust source ref: $line"; rc=1; continue
        fi
        resolve_rust_ref "$area" "$src" || { rc=1; continue; }
        count=$((count + 1))

        # Resolve any further crates/... references in the remaining columns.
        local extra
        for extra in $(echo "$line" | tr '`|' '  ' | tr ' ' '\n' | grep '^crates/' || true); do
            resolve_rust_ref "$area" "$extra" || rc=1
        done
    done < "$tfile"

    if [ "$rc" -eq 0 ]; then
        note "$area traceability: PASS ($count rows resolve)"
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
            -a)
                # Fail closed: -a must carry a value, and a value that looks
                # like the next flag ('-a -h') is a missing value, not an area.
                case "${2:-}" in
                    ''|-*) die "-a requires an AREA value" ;;
                esac
                only_area="$2"; shift 2 ;;
            -h|--help) usage 0 ;;
            *) die "unexpected argument: $1" ;;
        esac
    done

    case "$cmd" in
        smoke|exhaustive|negative|traceability|ci|all) : ;;
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
        *) ensure_jar; note "tlc: $JAR (tla2tools $TLA_VERSION, sha256 verified)" ;;
    esac

    RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse 'HEAD^{tree}')"

    local records_tsv=0
    case "$cmd" in smoke|exhaustive|negative|ci|all) records_tsv=1 ;; esac
    [ "$records_tsv" -eq 1 ] && truncate_tsv

    # ci and all record every model under ONE run id, so last-run.tsv is a
    # single coherent run: the smoke, negative, and (for all) exhaustive rows
    # all carry the same run-id column.
    local rc=0 area
    for area in $areas; do
        case "$cmd" in
            smoke)        check_model "$area" smoke || rc=1 ;;
            exhaustive)   check_model "$area" exhaustive || rc=1 ;;
            negative)     check_negative "$area" || rc=1 ;;
            traceability) check_traceability "$area" || rc=1 ;;
            ci)
                check_model "$area" smoke || rc=1
                check_negative "$area" || rc=1
                check_traceability "$area" || rc=1
                ;;
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
