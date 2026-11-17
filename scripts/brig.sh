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
# directory beside the primary checkout.
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

warn_disk() {
  free_gb=$(df -Pk "$1" | awk 'NR == 2 { print int($4 / 1048576) }')
  if [ -n "$free_gb" ] && [ "$free_gb" -lt 70 ]; then
    echo "brig: $free_gb GB free under $1. A cold build in the sandbox writes 54 GB or more to the clone's target/brig/." >&2
  fi
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
    # Objects come from the primary's store and are then copied, so the
    # clone shares nothing with it and the guest cannot reach it.
    git clone --quiet --reference-if-able "$common" --dissociate --no-checkout "$url" "$dir"
    git -C "$dir" switch --quiet -c "$1" origin/main
    git -C "$dir" config ravel.sandboxCheckout true
    echo "brig: sandbox clone $dir on branch $1. Start it with: make brig NAME=$1" >&2
    warn_disk "$clones"
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
    "$BRIG" rm --target "$target" -f "$brigfile" || [ $? -eq 3 ]
    BRIG_PULL=always
    export BRIG_PULL
    cmd=run
    ;;
  run | shell) ;;
  *) usage ;;
esac

warn_disk "$dir"
case $cmd in
  shell) set -- -- scripts/brig-dev.sh --shell "$@" ;;
  # With no Claude arguments, the target's own args, so the approval of a
  # plain make brig stays the same.
  run) [ $# -eq 0 ] || set -- -- scripts/brig-dev.sh "$@" ;;
esac
exec "$BRIG" run --target "$target" -f "$brigfile" ${image_flag:+"$image_flag"} "$@"
