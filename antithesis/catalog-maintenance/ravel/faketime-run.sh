#!/bin/sh
# Every container's entrypoint under the clock jump (config/docker-compose.yaml):
# refuse to start unless libfaketime is preloaded, reads the shared offset
# file, and can reach the clock reads of the process it wraps; then exec the
# container's own entrypoint. libfaketime itself fails open (a missing
# library or offset file leaves the real clock), so these checks are what
# make a container that would miss the jump fail to start instead.
#
# FAKETIME_CHECK_BINARY names the binary whose clock matters. A statically
# linked binary has no dynamic loader, so LD_PRELOAD never reaches it; the
# check is that its program header names the glibc loader.
lib=/faketime/libfaketimeMT.so.1
fail() {
    echo "faketime: $*" >&2
    exit 70
}
[ -r "$lib" ] || fail "$lib is not readable"
[ "${LD_PRELOAD:-}" = "$lib" ] || fail "LD_PRELOAD is '${LD_PRELOAD:-}', not $lib"
[ -n "${FAKETIME_TIMESTAMP_FILE:-}" ] && [ -r "$FAKETIME_TIMESTAMP_FILE" ] \
    || fail "FAKETIME_TIMESTAMP_FILE '${FAKETIME_TIMESTAMP_FILE:-}' is not readable"
# FAKETIME overrides the file for this one probe: one day ahead must read one
# day ahead, which only an interposed clock does.
base=$(FAKETIME=+0 date +%s) || fail "date failed"
ahead=$(FAKETIME=+1d date +%s) || fail "date failed"
shift_s=$((ahead - base))
[ "$shift_s" -ge 86000 ] && [ "$shift_s" -le 86800 ] \
    || fail "a one-day offset moved date by ${shift_s}s; libfaketime is not interposing"
if [ -n "${FAKETIME_CHECK_BINARY:-}" ]; then
    head -c 4096 "$FAKETIME_CHECK_BINARY" | grep -qa 'ld-linux-x86-64.so.2' \
        || fail "$FAKETIME_CHECK_BINARY does not load through the glibc dynamic loader, so LD_PRELOAD cannot reach its clock reads"
fi
echo "faketime: offset $(cat "$FAKETIME_TIMESTAMP_FILE"), reread every ${FAKETIME_CACHE_DURATION:-10}s"
exec "$@"
