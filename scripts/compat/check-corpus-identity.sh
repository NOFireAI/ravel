#!/usr/bin/env bash
# Corpus byte-identity check (ADR-4233 decision 9, issue #4399).
#
# A frozen corpus under crates/ravel-compat-tests/corpus/<release>/ is written
# before its release is tagged. At the tag, the release manager checks that the
# tagged build writes the same bytes: this script regenerates the release
# directory from scratch with `generate_corpus` and diffs it against the
# committed one. Run it on a checkout of the release tag (or the commit the
# release was cut from), then record the result in a new VERIFIED file in the
# release directory, as corpus/v0.34.0/VERIFIED does.
#
# Steps:
#   1. move the committed release directory aside, within
#      crates/ravel-compat-tests/ so the move is a rename;
#   2. seed a fresh directory with the committed PROVENANCE only
#      (`generate_corpus` ends by reading the directory back, which requires
#      PROVENANCE, and never writes it);
#   3. run the generator:
#        RAVEL_COMPAT_CORPUS_RELEASE=<release> cargo test \
#          --manifest-path crates/ravel-compat-tests/Cargo.toml \
#          --test corpus -- --ignored generate_corpus
#   4. diff -r -x PROVENANCE -x VERIFIED committed fresh, printing the diff;
#   5. restore the committed directory, on every exit path (EXIT trap; INT and
#      TERM exit through it). A SIGKILL cannot be trapped: the committed copy
#      is then left at the aside path printed in step 1, and
#      `git checkout -- <release dir>` restores it from git.
#
# The release directory must have no uncommitted changes, so the bytes compared
# and restored are the committed ones and git can always restore them.
#
# usage: scripts/compat/check-corpus-identity.sh RELEASE_DIR
#   RELEASE_DIR is a release directory name (v0.34.0) or its path
#   (crates/ravel-compat-tests/corpus/v0.34.0, or corpus/v0.34.0 relative to
#   crates/ravel-compat-tests). CARGO_BUILD_JOBS and the rest of the cargo
#   environment pass through.
#
# Exit codes:
#   0   the regenerated directory is byte-identical to the committed one,
#       every file other than PROVENANCE and VERIFIED compared
#   1   a difference (printed), a failed step, or a failed restore
#   64  bad usage: wrong argument count, no such release directory, no
#       PROVENANCE in it, or uncommitted changes under it

set -euo pipefail

usage() {
  echo "usage: $0 RELEASE_DIR" >&2
  exit 64
}

[[ $# -eq 1 && -n "$1" ]] || usage

repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel) || {
  echo "check-corpus-identity: cannot find the repository root" >&2
  exit 1
}
crate_rel=crates/ravel-compat-tests
crate_dir="$repo/$crate_rel"
corpus_root="$crate_dir/corpus"

release=$(basename "$1")
dir="$corpus_root/$release"
dir_rel="$crate_rel/corpus/$release"

if [[ "$release" == "." || "$release" == ".." || ! -d "$dir" ]]; then
  echo "check-corpus-identity: no release directory $dir_rel" >&2
  exit 64
fi
if [[ ! -f "$dir/PROVENANCE" ]]; then
  echo "check-corpus-identity: $dir_rel has no PROVENANCE" >&2
  exit 64
fi
dirty=$(git -C "$repo" status --porcelain -- "$dir_rel") || {
  echo "check-corpus-identity: git status failed for $dir_rel" >&2
  exit 1
}
if [[ -n "$dirty" ]]; then
  echo "check-corpus-identity: $dir_rel has uncommitted changes:" >&2
  echo "$dirty" >&2
  exit 64
fi

aside=$(mktemp -d "$crate_dir/.corpus-identity-$release.XXXXXX") || {
  echo "check-corpus-identity: cannot create the aside directory" >&2
  exit 1
}
committed="$aside/$release"
moved=0

restore() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ $moved -eq 1 ]]; then
    if rm -rf "$dir" && mv "$committed" "$dir" && rmdir "$aside"; then
      echo "check-corpus-identity: restored $dir_rel"
    else
      echo "check-corpus-identity: RESTORE FAILED; the committed copy is at" \
        "$committed (or run: git checkout -- $dir_rel)" >&2
      rc=1
    fi
  else
    rmdir "$aside" 2>/dev/null || true
  fi
  exit "$rc"
}
trap restore EXIT
trap 'exit 1' INT TERM

mv "$dir" "$committed"
moved=1
echo "check-corpus-identity: committed $dir_rel moved aside to $committed"

mkdir "$dir"
cp -p "$committed/PROVENANCE" "$dir/PROVENANCE"

echo "check-corpus-identity: regenerating $release with generate_corpus"
gen_rc=0
RAVEL_COMPAT_CORPUS_RELEASE="$release" cargo test \
  --manifest-path "$crate_dir/Cargo.toml" \
  --test corpus -- --ignored generate_corpus || gen_rc=$?
if [[ $gen_rc -ne 0 ]]; then
  echo "check-corpus-identity: FAIL: generate_corpus exited $gen_rc" >&2
  exit 1
fi

compared=$(cd "$committed" && find . -type f ! -path ./PROVENANCE ! -path ./VERIFIED | wc -l)
if [[ $compared -eq 0 ]]; then
  echo "check-corpus-identity: FAIL: the committed directory has no files to compare" >&2
  exit 1
fi

diff_rc=0
diff -r -x PROVENANCE -x VERIFIED "$committed" "$dir" || diff_rc=$?
if [[ $diff_rc -ne 0 ]]; then
  echo "check-corpus-identity: FAIL: the regenerated $release differs from" \
    "the committed files (diff exit $diff_rc, output above)" >&2
  exit 1
fi

echo "check-corpus-identity: PASS: $release regenerated byte-identical," \
  "$compared files compared (PROVENANCE and VERIFIED excluded)"
