#!/usr/bin/env bash
# Point this checkout's git hooks at the tracked .githooks/ directory.
#
# core.hooksPath is per-repository config and it is SHARED by every linked
# worktree, so one run covers the primary checkout and every worktree cut
# from it, now and later. It is not committed, which is why this script
# exists: a tracked hook directory that nobody points git at protects
# nothing.
#
# Usage: install-git-hooks.sh [--check]
#   --check  report whether the hooks are installed; exit 1 if they are not.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
common_dir="$(git rev-parse --git-common-dir)"
[[ "${common_dir}" != /* ]] && common_dir="${repo_root}/${common_dir}"
primary="$(cd "$(dirname "${common_dir}")" && pwd)"
want=".githooks"

current="$(git config --get core.hooksPath 2>/dev/null || true)"

if [[ "${1:-}" == "--check" ]]; then
  if [[ "${current}" == "${want}" ]]; then
    echo "git hooks: installed (core.hooksPath=${current})"
    exit 0
  fi
  echo "git hooks: NOT installed (core.hooksPath='${current:-unset}')." >&2
  echo "  The destructive-push guard in .githooks/pre-push is inert. Run: scripts/guards/install-git-hooks.sh" >&2
  exit 1
fi

if [[ -n "${current}" && "${current}" != "${want}" ]]; then
  echo "install-git-hooks.sh: core.hooksPath is already '${current}'." >&2
  echo "  Refusing to overwrite it. Move those hooks into ${primary}/.githooks/ and re-run, or unset it first." >&2
  exit 65
fi

chmod +x "${primary}/.githooks/"* 2>/dev/null || true
git config core.hooksPath "${want}"
echo "git hooks: core.hooksPath=${want} (covers ${primary} and every worktree cut from it)"
echo "  pre-push refuses a history-dropping push to main or to a branch with an open pull request."
echo "  Override for one push with ALLOW_DESTRUCTIVE=1."
