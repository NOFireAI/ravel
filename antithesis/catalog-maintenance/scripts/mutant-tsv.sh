# Sourced, not run: the mutant areas this workload runs and where each
# area's mutant names live (ROOT/formal/tla/<area>/antithesis-mutants.tsv,
# column 2 of a line whose column 2 starts with <area>/; a none: line names
# no mutant). mutant-config.sh, local-run.sh and build.sh read the names
# through these functions.
# shellcheck shell=bash

MUTANT_AREAS="catalog resharding maintenance"
MUTANT_TSV_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

# Prints every mutant name of every area in MUTANT_AREAS, one per line.
# Returns 1, with the reason on stderr, when an area's tsv cannot be read.
mutant_names() {
    local area tsv
    for area in $MUTANT_AREAS; do
        tsv="$MUTANT_TSV_ROOT/formal/tla/$area/antithesis-mutants.tsv"
        if [ ! -r "$tsv" ]; then
            echo "cannot read $tsv" >&2
            return 1
        fi
        awk -F '\t' -v p="$area/" 'index($2, p) == 1 { print $2 }' "$tsv"
    done
}

# Prints the tsv path of the area a name area/stem names. Returns 1, with the
# reason on stderr, when the area is not in MUTANT_AREAS or its tsv cannot be
# read.
mutant_tsv() {
    local name="$1" area tsv a
    case "$name" in
        */*) area="${name%%/*}" ;;
        *) echo "'$name' is not of the form area/stem" >&2; return 1 ;;
    esac
    for a in $MUTANT_AREAS; do
        if [ "$a" = "$area" ]; then
            tsv="$MUTANT_TSV_ROOT/formal/tla/$area/antithesis-mutants.tsv"
            if [ ! -r "$tsv" ]; then
                echo "cannot read $tsv" >&2
                return 1
            fi
            echo "$tsv"
            return 0
        fi
    done
    echo "area '$area' of '$name' is not one this workload runs ($MUTANT_AREAS)" >&2
    return 1
}
