#!/bin/sh
# disk-watchdog.sh <scope-dir> [floor_gb] [warn_gb] [sample_seconds]
#
# Samples free space on the data volume and kills the cargo/rustc processes
# running under <scope-dir> when it falls below the floor. Killing one build
# costs a retry; letting the volume reach zero stops every Bash command in
# every session on this host, including the ones that would clean up
# (docs/guides/operations.md's disk section, and issue #1526's executor
# variant of the same failure).
#
# <scope-dir> is REQUIRED, and is the only thing that makes this safe on a
# machine several sessions share. A process is killed only when its own cwd,
# read from `lsof -a -d cwd`, lies inside that directory. Matching by command
# name instead kills every session's build and reports it as killing yours:
# two sessions independently wrote that version of this watchdog within an
# hour on 2026-09-09, and one of them killed the other's compile with it. On
# a shared box a cleanup action needs the same scoping care as any other
# destructive one, because it is one.
#
# WATCHDOG_DRY_RUN=1 names the pids it would kill and kills nothing. Use it to
# check the matcher: testing the kill path for real is how you learn it works
# by losing a build to it.
#
# On firing it writes a marker at <scope-dir>/.disk-watchdog-fired. A gate
# whose run overlaps that marker is INVALID, not red: nextest reports a
# SIGTERM'd test as "1 failed", which the next reader cannot tell from a
# genuine failure. The marker is written BEFORE the kills, because the moment
# this fires is the moment a write is least likely to succeed, and a
# zero-byte marker still says the watchdog was in play. Any marker from an
# earlier firing is removed when this arms, so a stale one can never label a
# later, valid gate as invalid.
#
# It does not touch the gate's exit code. That belongs to the tool that
# produced it, and a wrapper that edits it is the same "nothing ran, reported
# as a result" failure wearing different clothes.
set -u

SCOPE_ARG=${1:?usage: disk-watchdog.sh <scope-dir> [floor_gb] [warn_gb] [sample_s]}
# Resolve the scope to a physical path. `lsof` reports a process's cwd with
# every symlink already resolved, so a caller-supplied `/tmp/...` or
# `/var/...` would never prefix-match its `/private/tmp/...` answer on macOS,
# and the watchdog would silently match nothing and kill nothing. Session
# scratchpads live under exactly those paths, so this is the common case, not
# a corner.
SCOPE=$(cd "${SCOPE_ARG}" 2>/dev/null && pwd -P) || SCOPE=""
if [ -z "${SCOPE}" ]; then
  echo "watchdog: scope directory does not exist: ${SCOPE_ARG}" >&2
  exit 2
fi
floor_gb=${2:-9}
warn_gb=${3:-15}
sample=${4:-45}
marker="${SCOPE}/.disk-watchdog-fired"
warned=0
starved=0

# Arm: clear any marker from a previous firing, so its presence always refers
# to this run.
rm -f "${marker}" 2>/dev/null || true

free_gb() {
    # The volume backing the SCOPE, not a hardcoded one: what fills is the
    # target dir, and CARGO_TARGET_DIR need not sit on the same mount as the
    # system volume. `df -Pk` is POSIX and gives KiB, matching
    # check-disk-headroom.sh; `df -g` is a BSD spelling that errors on Linux,
    # where every sample would then read empty and the watchdog would loop
    # forever looking armed. Integer division floors, so the effective floor
    # is never higher than the documented one.
    df -Pk "${SCOPE}" 2>/dev/null | awk 'NR==2 {print int($4/1048576)}'
}

# The cargo/rustc processes whose cwd is inside SCOPE. Two stages on purpose:
# `ps comm` is the full toolchain path on macOS, so the basename decides what
# is a build, and lsof decides whose it is.
scoped_pids() {
    for pid in $(ps -eo pid,comm= | awk '{ n = split($2, p, "/"); if (p[n] == "cargo" || p[n] == "rustc") print $1 }'); do
        cwd=$(lsof -a -d cwd -p "${pid}" -Fn 2>/dev/null | sed -n 's/^n//p' | head -1)
        # The scope root itself, or a path UNDER it. A bare "${SCOPE}"*
        # prefix also matches a sibling whose name merely starts with the
        # same string, so a scope of .../wt matches a build in .../wt2 and
        # kills another session's work: the exact thing this script exists
        # to prevent. Subdirectories must still match, since a build's cwd
        # is normally a crate directory rather than the worktree root.
        case "${cwd}" in
            "${SCOPE}"|"${SCOPE}"/*) printf '%s ' "${pid}" ;;
        esac
    done
}

while :; do
    avail=$(free_gb)
    case "${avail}" in
        ''|*[!0-9]*)
            echo "watchdog: could not read free space, sample skipped" >&2
            sleep "${sample}"
            continue
            ;;
    esac

    if [ "${avail}" -lt "${floor_gb}" ]; then
        pids=$(scoped_pids | sed 's/ *$//')
        if [ "${WATCHDOG_DRY_RUN:-0}" = "1" ]; then
            # A dry run is a diagnostic: it answers "what would you kill right
            # now" and returns. It must terminate whether or not anything
            # matched, or the very invocation meant for checking the matcher
            # hangs on the answer "nothing", which is the answer being checked.
            if [ -n "${pids}" ]; then
                echo "watchdog: DRY RUN, ${avail} GB left, would kill: ${pids}"
            else
                echo "watchdog: DRY RUN, ${avail} GB left, no cargo/rustc under ${SCOPE}"
            fi
            exit 0
        fi
        if [ -z "${pids}" ]; then
            # Nothing of ours to kill YET. Do not exit: the common way to
            # arm this is alongside a gate that has not spawned cargo yet,
            # or during a gap between two of gates.sh's sequential lanes,
            # which is exactly when free space is lowest. Exiting here
            # disarms the watchdog at the moment it is most needed, and with
            # the same status a successful firing returns, so no caller can
            # tell the two apart. Say it once, then keep sampling.
            if [ "${starved}" -eq 0 ]; then
                echo "watchdog: ${avail} GB left, below the ${floor_gb} GB floor, but no cargo/rustc under ${SCOPE} yet; still watching"
                starved=1
            fi
            sleep "${sample}"
            continue
        fi
        starved=0
        # Marker first: see the header.
        {
            echo "fired_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
            echo "free_gb=${avail}"
            echo "floor_gb=${floor_gb}"
            echo "killed_pids=${pids}"
            echo "note=any gate run overlapping this window is INVALID, not red;"
            echo "note=a SIGTERM'd test is reported as a failure by the runner."
        } > "${marker}" 2>/dev/null || true
        echo "watchdog: ${avail} GB left, below the ${floor_gb} GB floor; killing cargo/rustc under ${SCOPE}: ${pids}"
        # shellcheck disable=SC2086
        kill -TERM ${pids} 2>/dev/null || true
        sleep 5
        # shellcheck disable=SC2086
        kill -KILL ${pids} 2>/dev/null || true
        echo "watchdog: killed; marker at ${marker}; free space now $(free_gb) GB."
        exit 0
    fi

    if [ "${avail}" -lt "${warn_gb}" ] && [ "${warned}" -eq 0 ]; then
        echo "watchdog: ${avail} GB left, under the ${warn_gb} GB warning line, floor is ${floor_gb}"
        warned=1
    fi

    sleep "${sample}"
done
