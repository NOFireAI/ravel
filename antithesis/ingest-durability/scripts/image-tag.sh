# shellcheck shell=bash disable=SC2034
# Sourced by build.sh, export-images.sh and import-images.sh: picks the image
# tag from MUTANTS and TAG, and refuses a pairing that would put the mutant
# build under a plain tag or a plain build under a -mutants tag (ADR-4326
# decision 3: only a <workload>-mutants tag carries antithesis-mutants).
# Sets MUTANTS (1 or empty), TAG, and ARCHIVE_SUFFIX, which keeps the mutant
# archives apart from the plain ones under one S3 prefix.
MUTANTS="${MUTANTS:-}"
case "$MUTANTS" in
    1) TAG="${TAG:-ingest-durability-mutants}"; ARCHIVE_SUFFIX=-mutants ;;
    '') TAG="${TAG:-ingest-durability}"; ARCHIVE_SUFFIX="" ;;
    *) echo "refused: MUTANTS must be 1 or unset, not '$MUTANTS'" >&2; exit 1 ;;
esac
case "$TAG" in
    *-mutants)
        [ "$MUTANTS" = 1 ] || { echo "refused: TAG=$TAG needs MUTANTS=1" >&2; exit 1; } ;;
    *)
        [ "$MUTANTS" != 1 ] || { echo "refused: MUTANTS=1 needs a TAG ending in -mutants, not $TAG" >&2; exit 1; } ;;
esac
