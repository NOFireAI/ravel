#!/bin/sh
# assert-clean-authorship.sh <ref> [expected-email]
# Make sure that every commit on <ref> that is not on origin/main has the
# expected author, committer, and Signed-off-by, and no AI attribution
# trailer. Exit 0 when clean; exit 1 with the offending commits listed.
#
# fleet-result-merge.sh runs this after its authorship rewrite. Run it
# yourself on any branch you amended by hand, before you push: a wrong
# identity on protected main cannot be fixed later. The default expected
# email is `git config user.email`.
set -u

ref=${1:-}
expected=${2:-}

if [ -z "$ref" ]; then
  echo "guard: usage: assert-clean-authorship.sh <ref> [expected-email]" >&2
  exit 1
fi

if [ -z "$expected" ]; then
  expected=$(git config user.email 2>/dev/null) || expected=''
fi
if [ -z "$expected" ]; then
  echo "guard: no expected email: pass one or set git config user.email" >&2
  exit 1
fi

base=''
base=$(git merge-base origin/main "$ref" 2>/dev/null) || {
  echo "guard: cannot compute merge-base of origin/main and $ref" >&2
  exit 1
}

fail=0

bad_ids=''
bad_ids=$(git log --format='%h %ae %ce' "${base}..${ref}" \
  | awk -v want="$expected" '$2 != want || $3 != want') || bad_ids=''
if [ -n "$bad_ids" ]; then
  echo "guard: commits with wrong author/committer (want $expected):" >&2
  printf '%s\n' "$bad_ids" >&2
  fail=1
fi

for c in $(git rev-list "${base}..${ref}"); do
  body=$(git log -1 --format=%B "$c")
  printf '%s\n' "$body" | grep -qi "^Signed-off-by:.*<${expected}>" || {
    echo "guard: $c missing Signed-off-by <${expected}>" >&2
    fail=1
  }
  wrong_signoff=''
  wrong_signoff=$(printf '%s\n' "$body" \
    | grep -i '^Signed-off-by:' | grep -v "<${expected}>") || wrong_signoff=''
  if [ -n "$wrong_signoff" ]; then
    echo "guard: $c carries a foreign sign-off:" >&2
    printf '%s\n' "$wrong_signoff" >&2
    fail=1
  fi
  ai_trailer=''
  ai_trailer=$(printf '%s\n' "$body" \
    | grep -iE '^Co-Authored-By:.*(claude|anthropic)|Generated with|Claude-Session:') || ai_trailer=''
  if [ -n "$ai_trailer" ]; then
    echo "guard: $c carries an AI attribution trailer:" >&2
    printf '%s\n' "$ai_trailer" >&2
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "guard: authorship check FAILED for ${base}..${ref}." >&2
  echo "guard: rewrite the branch with the correct identity before you push." >&2
  exit 1
fi

count=$(git rev-list --count "${base}..${ref}")
echo "guard: authorship clean on ${count} commit(s) (${base}..${ref})"
exit 0
