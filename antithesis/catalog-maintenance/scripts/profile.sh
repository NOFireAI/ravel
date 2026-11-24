# Sourced, not run: the workload profile local-run.sh, mutant-config.sh and
# launch.sh run, chosen by WORKLOAD_PROFILE (default catalog-maintenance).
# Every profile runs this directory's images, driver and scripts; a profile
# has its own config directory (antithesis/<profile>/config), test name and
# owned areas (ADR-4326, per-workload amendment).
#
#   PROFILE_DIR         antithesis/<profile>
#   PROFILE_TEST_NAME   the Antithesis test name of a plain run; a mutant run
#                       appends -mutant
#   PROFILE_OWNED_TSV   the areas whose formal/tla/<area>/antithesis-mutants.tsv
#                       mutants run on this profile: their literals'
#                       `<area>: ` prefixes are the areas it owns
#   PROFILE_PENDING     owned always-assertions that only a forced situation
#                       not built yet reaches, one per line: listed by
#                       local-run.sh, never gated
# shellcheck shell=bash
# The variables are for the scripts that source this file.
# shellcheck disable=SC2034

WORKLOAD_PROFILE="${WORKLOAD_PROFILE:-catalog-maintenance}"
PROFILE_PENDING=""
case "$WORKLOAD_PROFILE" in
    catalog-maintenance)
        PROFILE_TEST_NAME=ravel-catalog-maintenance
        PROFILE_OWNED_TSV="catalog"
        ;;
    reshard-ownership)
        PROFILE_TEST_NAME=ravel-reshard-ownership
        # It runs the whole catalog-maintenance workload as well, so it owns
        # the catalog area too.
        PROFILE_OWNED_TSV="catalog resharding maintenance"
        # Reached only when a maintain worker warm-starts from a peer's memo
        # snapshot, which the local loop does not force
        # (antithesis/reshard-ownership/README.md, "The expected set").
        PROFILE_PENDING="maintenance: a seeded memo entry is never fresher than its snapshot"
        ;;
    *)
        echo "refused: WORKLOAD_PROFILE is '$WORKLOAD_PROFILE', not catalog-maintenance or reshard-ownership" >&2
        # shellcheck disable=SC2317 # return when sourced, exit otherwise
        return 1 2> /dev/null || exit 1
        ;;
esac
PROFILE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../$WORKLOAD_PROFILE" && pwd)"
