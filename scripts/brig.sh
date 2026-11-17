#!/bin/sh
# Entry point for the `make brig*` targets, on the host. Run it from a
# trusted checkout: the primary checkout or a worktree, never a sandbox
# clone. See docs/internal/dev-in-brig.md.
#
#   brig.sh new NAME              clone origin/main into a sandbox clone on branch NAME
#   brig.sh run NAME [claude...]  Claude Code in the sandbox of that clone
#   brig.sh shell NAME [bash...]  a login shell there instead
#   brig.sh plan NAME             what a run gets, booting nothing
#   brig.sh stop NAME | rm NAME   stop the sandbox, or remove it and its guest home
#   brig.sh update NAME           remove the sandbox, pull the image again, run
#   brig.sh login                 store the secrets the routes send
#   brig.sh path NAME             print the clone's path
#
# Sandbox clones live in $RAVEL_BRIG_CLONES, by default a store-brig
# directory beside the primary checkout. new, run, shell and update refuse
# when the disk is short or another Ravel sandbox is running: see need_disk
# and need_alone below for the limits and the variables that lift them.
set -eu

BRIG=${BRIG:-brig}
target=dev
gh_secret=ravel-gh-token

fail() {
  echo "brig: $*" >&2
  exit 1
}

usage() {
  sed -n '6,13p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 64
}

here=$(cd "$(dirname "$0")/.." && pwd)

# The guest can rewrite a sandbox clone's scripts and Makefile, and Brig's
# approval covers only the Brigfile and what it names. So the host runs this
# script from a checkout the guest cannot write.
if [ "$(git -C "$here" config --bool ravel.sandboxCheckout 2> /dev/null || true)" = true ]; then
  fail "$here is a sandbox clone. Run make brig from your primary checkout or a worktree instead."
fi

common=$(cd "$here" && cd "$(git rev-parse --git-common-dir)" && pwd)
primary=$(dirname "$common")
clones=${RAVEL_BRIG_CLONES:-$(dirname "$primary")/store-brig}

need_brig() {
  command -v "$BRIG" > /dev/null 2>&1 ||
    fail "brig is not installed. Install it from the main channel: brew install --cask brig-sh/brig/brig@main"
  "$BRIG" run --help 2>&1 | grep -q -- '--target' ||
    fail "$("$BRIG" version 2>&1 | head -n 1) cannot run a Brigfile target. Install brig from the main channel: brew install --cask brig-sh/brig/brig@main"
  # A repository profile cannot choose the hypervisor, and the routes need hvi.
  if [ "$(uname -s)" = Darwin ]; then
    BRIG_HYPERVISOR=hvi
    export BRIG_HYPERVISOR
  fi
}

clone_of() {
  case $1 in
    '' | -* | */* | .*) fail "NAME must be a plain name, such as 4405-fix-fold" ;;
  esac
  dir="$clones/$1"
}

need_clone() {
  clone_of "$1"
  [ -d "$dir/.git" ] || fail "no sandbox clone at $dir. Make one with: make brig-new NAME=$1"
  [ "$(git -C "$dir" config --bool ravel.sandboxCheckout 2> /dev/null || true)" = true ] ||
    fail "$dir is not a sandbox clone"
}

# Each clone builds into its own target/brig/ on the host disk, and nothing is
# shared between clones. A cold build writes 54 to 70 GB, and a full disk
# stops every session on the host, not only the sandbox. So a clone with no
# build yet needs RAVEL_BRIG_MIN_FREE_GB free, and one that has built needs
# RAVEL_BRIG_WARM_FREE_GB for what it adds.
min_free_gb=${RAVEL_BRIG_MIN_FREE_GB:-70}
warm_free_gb=${RAVEL_BRIG_WARM_FREE_GB:-20}

need_disk() { # need_disk DIR GB WHAT
  free_gb=$(df -Pk "$1" 2> /dev/null | awk 'NR == 2 { print int($4 / 1048576) }')
  [ -n "$free_gb" ] || fail "could not read the free space under $1"
  [ "$free_gb" -ge "$2" ] && return 0
  fail "$free_gb GB free under $1, and $3 needs $2 GB. Each clone builds into its own target/brig/, and a cold build writes 54 to 70 GB. Remove the clones of merged tickets (make brig-rm NAME=<name>, then delete $clones/<name>), or set RAVEL_BRIG_MIN_FREE_GB and RAVEL_BRIG_WARM_FREE_GB to run anyway."
}

# One Ravel sandbox runs at a time: each takes 8 vCPUs and 12 GB of memory,
# and each builds into its own target/brig/. "Could not check" refuses too.
need_alone() {
  [ "${RAVEL_BRIG_ALLOW_PARALLEL:-}" = 1 ] && return 0
  command -v jq > /dev/null 2>&1 ||
    fail "jq is needed to check for another running Ravel sandbox. Install it, or set RAVEL_BRIG_ALLOW_PARALLEL=1."
  listing=$("$BRIG" --json ls) ||
    fail "could not list sandboxes with brig --json ls, so could not check for another running Ravel sandbox. Set RAVEL_BRIG_ALLOW_PARALLEL=1 to skip the check."
  real_clones=$(cd "$clones" && pwd -P)
  real_dir=$(cd "$dir" && pwd -P)
  others=$(printf '%s\n' "$listing" | jq -r --arg c "$clones/" --arg rc "$real_clones/" --arg me "$dir" --arg rme "$real_dir" '
    .data.sandboxes[]
    | select(.state == "running")
    | .project.root // empty
    | select((startswith($c) or startswith($rc)) and . != $me and . != $rme)') ||
    fail "could not read the output of brig --json ls. Set RAVEL_BRIG_ALLOW_PARALLEL=1 to skip the check."
  [ -z "$others" ] && return 0
  fail "another Ravel sandbox is running: $others. Run one at a time: each takes 8 vCPUs, 12 GB of memory and its own target/brig/. Stop it with make brig-stop NAME=<name>, or set RAVEL_BRIG_ALLOW_PARALLEL=1."
}

cmd=${1:-}
case $cmd in
  new | path | login | run | shell | plan | stop | rm | update) ;;
  *) usage ;;
esac
shift

case $cmd in
  new)
    [ $# -eq 1 ] || usage
    clone_of "$1"
    [ ! -e "$dir" ] || fail "$dir already exists"
    url=$(git -C "$here" remote get-url origin)
    mkdir -p "$clones"
    need_disk "$clones" "$min_free_gb" "a new clone's first build"
    # Objects come from the primary's store and are then copied, so the
    # clone shares nothing with it and the guest cannot reach it.
    git clone --quiet --reference-if-able "$common" --dissociate --no-checkout "$url" "$dir"
    git -C "$dir" switch --quiet -c "$1" origin/main
    git -C "$dir" config ravel.sandboxCheckout true
    echo "brig: sandbox clone $dir on branch $1. Start it with: make brig NAME=$1" >&2
    echo "brig: its first build writes 54 to 70 GB to $dir/target/brig/. When the ticket merges, run make brig-rm NAME=$1 and delete $dir." >&2
    exit 0
    ;;
  path)
    [ $# -eq 1 ] || usage
    need_clone "$1"
    echo "$dir"
    exit 0
    ;;
  login)
    need_brig
    "$BRIG" secret import claude-code
    echo "brig: paste a fine-grained token for $gh_secret (see docs/internal/dev-in-brig.md), then Ctrl-D" >&2
    exec "$BRIG" secret create "$gh_secret"
    ;;
esac

[ $# -ge 1 ] || usage
name=$1
shift
need_brig
need_clone "$name"
brigfile="$dir/Brigfile"
[ -f "$brigfile" ] || fail "$brigfile is missing"

image_flag=
if [ -n "${IMAGE:-}" ]; then image_flag="--image=$IMAGE"; fi

# Checked before update removes anything, so a refused update leaves the
# sandbox as it was.
case $cmd in
  run | shell | update)
    if [ -d "$dir/target/brig" ]; then
      need_disk "$dir" "$warm_free_gb" "a build in a clone that has built before"
    else
      need_disk "$dir" "$min_free_gb" "a clone's first build"
    fi
    need_alone
    ;;
esac

case $cmd in
  plan)
    exec "$BRIG" plan --target "$target" -f "$brigfile" ${image_flag:+"$image_flag"}
    ;;
  stop)
    exec "$BRIG" stop --target "$target" -f "$brigfile"
    ;;
  rm)
    exec "$BRIG" rm --target "$target" -f "$brigfile"
    ;;
  update)
    # Brig pulls a tag only when the host lacks the image, and a sandbox keeps
    # the image it booted, so a new :latest needs both gone. Exit 3 is no
    # sandbox to remove.
    rm_code=0
    "$BRIG" rm --target "$target" -f "$brigfile" || rm_code=$?
    [ "$rm_code" -eq 0 ] || [ "$rm_code" -eq 3 ] || exit "$rm_code"
    BRIG_PULL=always
    export BRIG_PULL
    cmd=run
    ;;
  run | shell) ;;
  *) usage ;;
esac

case $cmd in
  shell) set -- -- scripts/brig-dev.sh --shell "$@" ;;
  # With no Claude arguments, the target's own args, so the approval of a
  # plain make brig stays the same.
  run) [ $# -eq 0 ] || set -- -- scripts/brig-dev.sh "$@" ;;
esac
exec "$BRIG" run --target "$target" -f "$brigfile" ${image_flag:+"$image_flag"} "$@"
