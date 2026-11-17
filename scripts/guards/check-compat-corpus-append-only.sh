#!/usr/bin/env bash
# Compat corpus is append-only (ADR-4233 decision 9, issue #4249).
#
# The frozen baseline corpus under crates/ravel-compat-tests/corpus/<release>/
# is written once, by a released build's real write paths, and every later
# release opens it and asserts exact query results. A fixture that is edited or
# removed after it is published stops being evidence of what that release wrote:
# the test would then compare the current build against bytes the current build
# was free to change. So a published corpus file may be ADDED (a new release's
# directory, or more fixtures under an in-flight one), never MODIFIED and never
# DELETED, relative to the merge base.
#
# This is a text/history scan: it compares the working range against the merge
# base and never builds. It is deliberately strict about the corpus tree only,
# and that tree includes each release's recorded results (expected.txt,
# samples.txt, versions.txt, logs/expected.txt, spans/expected.txt) and its
# PROVENANCE, which are frozen with the objects they describe. Everything else under crates/ravel-compat-tests/
# (the Rust tests, Cargo.toml) is free to change.
#
# Usage:
#   scripts/guards/check-compat-corpus-append-only.sh <base-ref> [<head-ref>]
#
# <head-ref> defaults to HEAD. The range checked is merge-base(base,head)..head,
# the same range a pull request's base..head covers, taken from the merge base
# so a corpus edit someone else pushed to the base branch is not attributed to
# this range (the lesson check-changelog-touched.sh records).
#
# Rename detection is deliberately OFF: a rename of a published corpus file
# removes its old path, which is a deletion of the published fixture whatever
# the new name is, and git then reports it as a delete (old) plus an add (new).
# The delete fails; the add is fine.
#
# Exit 0: no published corpus file was modified or deleted in the range (only
#         additions, or no corpus change at all).
# Exit 1: at least one published corpus file was modified or deleted. Each is
#         named with its change kind.
# Exit 2: the question could not be answered (missing argument, a ref git
#         cannot resolve, or a git command failing). Could-not-check is never
#         reported as a pass.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 2

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  awk 'NR > 1 && !/^#/ { exit } NR > 1' "$0"
  exit 0
fi

corpus_dir="crates/ravel-compat-tests/corpus"

base_ref="${1:-}"
head_ref="${2:-HEAD}"

if [[ -z "${base_ref}" ]]; then
  echo "check-compat-corpus-append-only.sh: missing <base-ref>" >&2
  exit 2
fi

base_sha="$(git rev-parse --verify "${base_ref}^{commit}" 2>/dev/null)" || {
  echo "check-compat-corpus-append-only.sh: cannot resolve base ref: ${base_ref}" >&2
  exit 2
}
head_sha="$(git rev-parse --verify "${head_ref}^{commit}" 2>/dev/null)" || {
  echo "check-compat-corpus-append-only.sh: cannot resolve head ref: ${head_ref}" >&2
  exit 2
}

merge_base="$(git merge-base "${base_sha}" "${head_sha}" 2>/dev/null)" || {
  echo "check-compat-corpus-append-only.sh: no merge base for ${base_sha} and ${head_sha}" >&2
  exit 2
}
if [[ -z "${merge_base}" ]]; then
  echo "check-compat-corpus-append-only.sh: empty merge base for ${base_sha} and ${head_sha}" >&2
  exit 2
fi
range="${merge_base}..${head_sha}"

# --no-renames so a renamed fixture shows as D(old)+A(new): the deletion fails.
if ! changed="$(git diff --name-status --no-renames "${range}" -- "${corpus_dir}/" 2>/dev/null)"; then
  echo "check-compat-corpus-append-only.sh: git diff failed for ${range}" >&2
  exit 2
fi

violations=0
while IFS=$'\t' read -r change path _rest; do
  [[ -z "${change}" ]] && continue
  case "${change}" in
    A) ;; # an added fixture is the only allowed change
    M)
      echo "check-compat-corpus-append-only.sh: MODIFIED published corpus file: ${path}" >&2
      violations=1
      ;;
    D)
      echo "check-compat-corpus-append-only.sh: DELETED published corpus file: ${path}" >&2
      violations=1
      ;;
    T)
      echo "check-compat-corpus-append-only.sh: TYPE-CHANGED published corpus file: ${path}" >&2
      violations=1
      ;;
    *)
      # C (copy), or any status not expected under --no-renames: treat an
      # unrecognised change to a published path as a finding, not a pass.
      echo "check-compat-corpus-append-only.sh: unexpected change (${change}) to published corpus file: ${path}" >&2
      violations=1
      ;;
  esac
done <<<"${changed}"

if [[ "${violations}" -ne 0 ]]; then
  echo "  Fix: a published compat corpus is frozen evidence. Add a new fixture" >&2
  echo "  or a new release directory under ${corpus_dir}/; never edit or remove one." >&2
  exit 1
fi

echo "check-compat-corpus-append-only.sh: clean (no published corpus file modified or deleted in ${range})"
exit 0
