#!/usr/bin/env bash
# Entry point of the Brigfile's dev target. Runs inside the Brig sandbox,
# never on the host: scripts/brig.sh starts it. See docs/internal/dev-in-brig.md.
#
#   brig-dev.sh [claude args...]   start Claude Code
#   brig-dev.sh --shell [args...]  start a login shell; `--shell -c CMD` runs one command
#
# Brig sets BRIG_PROJECT to the guest path of the project and runs this
# script with the target's arguments.
set -euo pipefail

fail() {
  echo "brig-dev: $*" >&2
  exit 1
}

[ -n "${BRIG_PROJECT:-}" ] || fail "runs inside a Brig sandbox only. On the host, run: make brig NAME=<name>"
cd "$BRIG_PROJECT"

if [ -f .git ]; then
  fail "$BRIG_PROJECT is a linked git worktree, and its .git file names a directory the sandbox does not mount. Make a sandbox clone with: make brig-new NAME=<name>"
fi

# The project arrives on a share owned by the host user, so git reports
# dubious ownership until the guest trusts it. The guest home survives
# brig stop, so the entry is added once. Run from $HOME: inside a project git
# cannot read, git stops before it reaches the global config.
if ! git rev-parse --git-dir > /dev/null 2>&1; then
  if ! git -C "$HOME" config --global --get-all safe.directory 2> /dev/null | grep -qxF "$BRIG_PROJECT"; then
    git -C "$HOME" config --global --add safe.directory "$BRIG_PROJECT"
  fi
fi
if ! err=$(git rev-parse --git-dir 2>&1 > /dev/null); then
  fail "git rev-parse --git-dir failed: $err"
fi
if ! git rev-parse --verify -q origin/main > /dev/null; then
  echo "brig-dev: origin/main is missing, and scoped gates diff against it. Fetch it with: git fetch origin main" >&2
fi

# The toolchain and the registry live under /usr/local, outside the guest
# home that Brig mounts over /root. Set here as well as in the image and the
# profile, so nothing depends on how the runtime passes either on.
export RUSTUP_HOME=/usr/local/rustup
export CARGO_HOME=/usr/local/cargo
export PATH="$CARGO_HOME/bin:$PATH"
export npm_config_cache=/opt/npm-cache
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"

# The guest root is a 16 GiB disk that brig stop throws away, and a cold
# gate needs far more. Build output goes to the project share instead, under
# target/ (gitignored), in a directory of its own so a host build in the same
# checkout never mixes macOS and Linux artifacts.
export CARGO_TARGET_DIR="$BRIG_PROJECT/target/brig"

channel=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' rust-toolchain.toml)
if [ -n "$channel" ] && ! rustup toolchain list | grep -q "^$channel-"; then
  echo "brig-dev: rust-toolchain.toml names $channel, which the image lacks; rustup installs it on the first cargo command" >&2
fi

if [ "${1:-}" = "--shell" ]; then
  shift
  exec bash -l "$@"
fi
exec claude "$@"
