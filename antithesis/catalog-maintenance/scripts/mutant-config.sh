#!/bin/bash
# Write a copy of the profile's config/ (WORKLOAD_PROFILE, scripts/profile.sh:
# this workload's, or antithesis/reshard-ownership/config) that runs one
# mutant (ADR-4326 decision 3): every
# image the workload builds names its catalog-maintenance-mutants tag, and
# RAVEL_ANTITHESIS_MUTANT names the mutant on every ravel-server container
# (ravel1, ravel2, maint1, maint2). launch.sh launches the copy; local-run.sh
# runs it under the local override.
#
# Usage: scripts/mutant-config.sh <mutant> <dest-dir>
#   mutant    one of the <area>/<stem> mutant names in
#             formal/tla/<area>/antithesis-mutants.tsv, for the areas
#             mutant-tsv.sh lists: catalog, resharding and maintenance
#   dest-dir  created; refused when it exists and is not empty
#
# Exit 0: the copy is written and selects the mutant. Exit 1: refused, with
# the reason on stderr; nothing usable is left in dest-dir.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=mutant-tsv.sh
. "$HERE/mutant-tsv.sh"
# The profile whose config/ is copied (WORKLOAD_PROFILE, default
# catalog-maintenance).
# shellcheck source=profile.sh
. "$HERE/profile.sh" || exit 1
MUTANT="${1:?mutant name}"
DEST="${2:?destination directory}"
PLAIN_TAG=catalog-maintenance
MUTANT_TAG=catalog-maintenance-mutants
IMAGES='^ *image: ravel-antithesis-(server|driver):'

refuse() {
    echo "refused: $*" >&2
    exit 1
}

names=$(mutant_names) || refuse "could not read the mutant names"
[ -n "$names" ] || refuse "the tsv files of $MUTANT_AREAS name no mutant"
if ! printf '%s\n' "$names" | grep -qxF -- "$MUTANT"; then
    refuse "'$MUTANT' is not a mutant of $MUTANT_AREAS; one of: $(printf '%s\n' "$names" | tr '\n' ' ')"
fi
# The name is written into YAML between double quotes below.
case "$MUTANT" in
    *[!a-z0-9/-]*) refuse "'$MUTANT' holds a character other than a-z, 0-9, / and -" ;;
esac

if [ -e "$DEST" ] && [ -n "$(ls -A "$DEST")" ]; then
    refuse "$DEST exists and is not empty"
fi
mkdir -p "$DEST"
cp -R "$PROFILE_DIR/config/." "$DEST/"
SRC="$DEST/docker-compose.yaml"
OUTF="$DEST/docker-compose.yaml.tmp"

plain_images=$(grep -cE "$IMAGES$PLAIN_TAG\$" "$SRC") || plain_images=0
selectors=$(grep -cxF '    RAVEL_ANTITHESIS_MUTANT: ""' "$SRC") || selectors=0
[ "$plain_images" -gt 0 ] || refuse "config/docker-compose.yaml names no ravel-antithesis image tagged $PLAIN_TAG"
[ "$selectors" -eq 1 ] || refuse "config/docker-compose.yaml has $selectors empty RAVEL_ANTITHESIS_MUTANT lines, not 1"

# sed -i is not portable (GNU takes no suffix, BSD/macOS requires one), so
# write a temp file and move it over.
sed -E \
    -e "s#($IMAGES)$PLAIN_TAG\$#\\1$MUTANT_TAG#" \
    -e "s#^    RAVEL_ANTITHESIS_MUTANT: \"\"\$#    RAVEL_ANTITHESIS_MUTANT: \"$MUTANT\"#" \
    "$SRC" > "$OUTF"
mv "$OUTF" "$SRC"

left=$(grep -cE "$IMAGES$PLAIN_TAG\$" "$SRC") || left=0
moved=$(grep -cE "$IMAGES$MUTANT_TAG\$" "$SRC") || moved=0
selected=$(grep -cxF "    RAVEL_ANTITHESIS_MUTANT: \"$MUTANT\"" "$SRC") || selected=0
if [ "$left" -ne 0 ] || [ "$moved" -ne "$plain_images" ] || [ "$selected" -ne 1 ]; then
    rm -f "$SRC"
    refuse "the rewrite of $SRC did not apply ($left plain image lines left, $moved of $plain_images moved to $MUTANT_TAG, $selected mutant selectors)"
fi
echo "mutant config: $MUTANT over the $MUTANT_TAG images ($moved image lines) in $DEST"
