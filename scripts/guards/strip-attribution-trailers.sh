#!/usr/bin/env bash
# Print the commit message on stdin without its AI attribution lines.
#
#   git log -1 --format=%B <sha> | scripts/guards/strip-attribution-trailers.sh
#
# The lines removed are exactly the ones assert-clean-authorship.sh refuses:
# a Co-Authored-By naming Claude or Anthropic, a "Generated with" line, and
# a Claude-Session line. A Co-Authored-By naming a person is kept.
# fleet-result-merge.sh runs this while it re-authors a result branch, so an
# executor that added such a line does not fail the branch at the guard.
#
# Exit 0 always, also when nothing matched or the input was empty: the output
# is the message to commit, and an empty one is the caller's to refuse.
# strip-attribution-trailers.test.sh checks the two scripts agree, by running
# the guard over commits built from this script's output.
set -euo pipefail

# Keep in step with the pattern in assert-clean-authorship.sh.
PATTERN='^Co-Authored-By:.*(claude|anthropic)|^(🤖 )?Generated with|^Claude-Session:'

grep -viE "${PATTERN}" || true
