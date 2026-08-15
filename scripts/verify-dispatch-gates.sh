#!/usr/bin/env bash
# Tier-1 deterministic verification for a fleet-dispatched branch: an
# isolated worktree, a cold CARGO_TARGET_DIR, and the full workspace gate
# list. Not a crate-scoped subset, and not the repo's warm shared target
# dir: either shortcut can pass a broken branch, because a crate-scoped
# clippy run misses a cross-crate break in an untouched crate, and a warm
# incremental target dir can mask a real compile error behind stale
# cached artifacts (see the verify-dispatch skill).
#
# Usage: verify-dispatch-gates.sh <ref> <worktree-parent-dir>
#
# <ref> is anything `git worktree add` accepts: a branch, a tag, a SHA.
# <worktree-parent-dir> MUST be outside this repo's working tree: an
# untracked worktree left inside the repo blocks the next fleet_dispatch,
# which refuses to run against a dirty primary checkout.
#
# Prints each gate command as it runs. Exits 0 only if every gate passes;
# on the first failure, prints which command failed and its exit code,
# then exits with that code. Always removes the worktree on exit, success
# or failure.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <ref> <worktree-parent-dir>" >&2
  exit 64
fi

ref="$1"
parent_dir="$2"

repo_root="$(git rev-parse --show-toplevel)"
mkdir -p "${parent_dir}"
parent_dir_abs="$(cd "${parent_dir}" && pwd)"
case "${parent_dir_abs}" in
  "${repo_root}" | "${repo_root}"/*)
    echo "verify-dispatch-gates.sh: worktree-parent-dir must be outside the repo tree (${repo_root}), got: ${parent_dir_abs}" >&2
    exit 65
    ;;
esac

sha="$(git rev-parse "${ref}")"
short_sha="$(git rev-parse --short "${ref}")"
worktree_dir="${parent_dir_abs}/verify-${short_sha}"

cleanup() {
  git worktree remove "${worktree_dir}" --force >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> Resolved ref ${ref} to ${sha}"
echo "==> Creating worktree at ${worktree_dir}"
# Check out the resolved SHA (detached), not the ref name: if ${ref} is a
# branch already checked out elsewhere (including main in the primary
# checkout), `git worktree add <path> <branch>` refuses with "already
# used by worktree". Checking out the SHA directly sidesteps that and is
# the more correct thing for a verification run to do anyway: it pins the
# exact commit being verified rather than tracking a branch that could move.
git worktree add --detach "${worktree_dir}" "${sha}"

export CARGO_TARGET_DIR="${worktree_dir}/target"
echo "==> Cold CARGO_TARGET_DIR: ${CARGO_TARGET_DIR}"

run_gate() {
  echo "==> (cd ${worktree_dir} && $*)"
  local code=0
  (cd "${worktree_dir}" && "$@") || code=$?
  if [[ ${code} -ne 0 ]]; then
    echo "TIER1 FAIL: '$*' exited ${code}" >&2
    return "${code}"
  fi
  return 0
}

# --locked pins the committed Cargo.lock so this cold-cache verification
# resolves exactly what CI does; a divergent resolve here would defeat the
# purpose of the isolated run. `cargo fmt` (rustfmt) rejects --locked after
# the subcommand, so it takes the global-flag position.
run_gate cargo --locked fmt --all --check
run_gate cargo build --locked --workspace --all-targets
run_gate cargo clippy --locked --workspace --all-targets -- -D warnings
run_gate cargo test --locked --workspace
run_gate cargo test --locked --doc --workspace

echo "TIER1 PASS"
