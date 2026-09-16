#!/usr/bin/env bash
# Tier-1 deterministic verification for a fleet-dispatched branch: an
# isolated worktree, a cold CARGO_TARGET_DIR, and the full workspace gate
# list. Not a crate-scoped subset, and not the repo's warm shared target
# dir: either shortcut can pass a broken branch, because a crate-scoped
# clippy run misses a cross-crate break in an untouched crate, and a warm
# incremental target dir can mask a real compile error behind stale
# cached artifacts (see the verify-dispatch skill).
#
# Usage: verify-dispatch-gates.sh [--with-gates] <ref> <worktree-parent-dir>
#
# <ref> is anything `git worktree add` accepts: a branch, a tag, a SHA.
# <worktree-parent-dir> MUST be outside this repo's working tree: a
# worktree left inside the repo shows up as untracked content on the
# primary checkout, gets swept into any dispatch that takes local HEAD
# implicitly, and pollutes every `git status` a session reads (the
# dispatch push itself is ref-based and does not refuse on it; #687).
#
# Prints each gate command as it runs. Exits 0 only if every gate passes;
# on the first failure, prints which command failed and its exit code,
# then exits with that code. Always removes the worktree on exit, success
# or failure.
#
# --with-gates (or VERIFY_WITH_GATES=1): run scripts/gates.sh, unscoped,
# from inside the cold worktree instead of the five hand-listed cargo
# commands below. gates.sh also covers the sql / flight-sql / ravel-bench
# feature lanes the five commands do not, and on a clean tree it writes a
# gates-pass receipt keyed by tree hash (see gates.sh's "Gates-pass
# receipt" section) that fleet-result-merge.sh's FLEET_MERGE_SKIP_GATES=1
# reads to skip its own second full build. Without this flag, behavior is
# byte-for-byte unchanged from before it existed.
#
# GATES_SH overrides the path to gates.sh (default: the worktree's own
# scripts/gates.sh, so a branch that edits gates.sh is verified against
# its own edit). Test-only escape hatch for substituting a stub.
set -euo pipefail

with_gates=0
positional=()
for arg in "$@"; do
  case "${arg}" in
    --with-gates) with_gates=1 ;;
    *) positional+=("${arg}") ;;
  esac
done
if [[ "${VERIFY_WITH_GATES:-0}" == "1" ]]; then
  with_gates=1
fi
set -- "${positional[@]}"

if [[ $# -lt 2 ]]; then
  echo "usage: $0 [--with-gates] <ref> <worktree-parent-dir>" >&2
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
# Unique per invocation, not per ref. `verify-${short_sha}` collides whenever
# two runs verify the same ref, which is the normal case: a result branch gets
# verified, comes back with a finding, and is verified again while the first
# run is still building. The second run's `git worktree add` then fails with
# "already exists" and its EXIT trap removes the FIRST run's worktree, taking
# an in-flight cold build with it. That surfaces as a corrupted gate rather
# than as a collision, and on this machine it costs about an hour.
worktree_dir="${parent_dir_abs}/verify-${short_sha}-$$-$(date +%s)"

# Set only once this invocation has actually created the worktree. An
# unconditional cleanup installed before creation is what let a failed
# `worktree add` delete a directory this run did not make: the trap cannot
# tell "mine, remove it" from "someone else's, leave it" unless creation is
# what records ownership.
#
# No test discriminates on this alone, and that is a property of the fix
# rather than a gap someone forgot to close: with the per-invocation naming
# above, `worktree_dir` cannot name a directory this run did not create, so
# the situation this guard changes is unreachable. It is defence in depth
# against the naming being weakened later, and the suite's case (e) says so
# explicitly rather than appearing to cover it. Do not delete this as dead
# code; delete it only if you also intend the name to go back to being
# shared, and then case (d) fails.
created_worktree=""
cleanup() {
  [[ -n "${created_worktree}" ]] || return 0
  git worktree remove "${created_worktree}" --force >/dev/null 2>&1 || true
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
# Creation succeeded, so this run owns that directory and may remove it.
created_worktree="${worktree_dir}"

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

if [[ ${with_gates} -eq 1 ]]; then
  gates_sh="${GATES_SH:-${worktree_dir}/scripts/gates.sh}"
  run_gate "${gates_sh}"

  tree_hash="$(git -C "${worktree_dir}" rev-parse 'HEAD^{tree}')"
  receipt_dir="$(cd "$(git -C "${worktree_dir}" rev-parse --git-common-dir)" && pwd)/gates-pass"
  echo "==> Gates receipt: ${receipt_dir}/${tree_hash}"
else
  # --locked pins the committed Cargo.lock so this cold-cache verification
  # resolves exactly what CI does; a divergent resolve here would defeat
  # the purpose of the isolated run. `cargo fmt` (rustfmt) rejects --locked
  # after the subcommand, so it takes the global-flag position.
  run_gate cargo --locked fmt --all --check
  run_gate cargo build --locked --workspace --all-targets
  run_gate cargo clippy --locked --workspace --all-targets -- -D warnings
  run_gate cargo test --locked --workspace
  run_gate cargo test --locked --doc --workspace
fi

echo "TIER1 PASS"
