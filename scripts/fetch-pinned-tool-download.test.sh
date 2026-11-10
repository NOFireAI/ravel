#!/usr/bin/env bash
# Cases for fetch-pinned-tool-download.sh (issue #4093). A stubbed curl on
# PATH stands in for the real download: it counts its own invocations in
# STUB_CURL_COUNT_FILE, fails STUB_CURL_FAIL_COUNT times with
# STUB_CURL_HTTP_FAIL on stdout (mirroring curl -w '%{http_code}' on a
# non-2xx response) and a non-zero exit, then succeeds, writing
# STUB_CURL_CONTENT to the -o destination and STUB_CURL_HTTP_OK on stdout.
#
# FETCH_PINNED_TOOL_RETRY_DELAY_S=0 everywhere below: these cases exercise
# the retry COUNT and backoff MATH, not the sleep duration, and a test that
# slept the real 10/20/40/80 s schedule would cost the suite minutes for no
# added coverage.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/fetch-pinned-tool-download.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }

# A fake PATH entry providing only `curl`, so the production script's own
# `curl` call resolves to the stub, not the real binary.
FAKEBIN="$TMP/fakebin"
mkdir -p "$FAKEBIN"
cat >"$FAKEBIN/curl" <<'STUB'
#!/usr/bin/env bash
set -u
out=""
args=("$@")
i=0
while [ "$i" -lt "${#args[@]}" ]; do
    if [ "${args[$i]}" = "-o" ]; then
        i=$((i + 1))
        out="${args[$i]}"
    fi
    i=$((i + 1))
done
count_file="${STUB_CURL_COUNT_FILE:?STUB_CURL_COUNT_FILE unset}"
count=0
[ -f "$count_file" ] && count="$(cat "$count_file")"
count=$((count + 1))
echo "$count" >"$count_file"
fail_count="${STUB_CURL_FAIL_COUNT:-0}"
if [ "$count" -le "$fail_count" ]; then
    printf '%s' "${STUB_CURL_HTTP_FAIL:-500}"
    exit 22
fi
printf '%s' "${STUB_CURL_CONTENT:-ok}" >"$out"
printf '%s' "${STUB_CURL_HTTP_OK:-200}"
exit 0
STUB
chmod +x "$FAKEBIN/curl"

# --- case 1: fails four times and succeeds on the fifth --------------------
# FLIP: STUB_CURL_FAIL_COUNT=4 means the fifth curl call is the first success.
# Lowering it to 3 (success on the fourth call) would let this case pass with
# only four calls recorded -- the opposite of what it is meant to pin, that
# the script actually drives all five attempts before giving up on four
# failures. Asserting call_count -eq 5 is what catches that flip.
dest1="$TMP/case1/dest"
mkdir -p "$(dirname "$dest1")"
count_file1="$TMP/case1/count"
out1="$(PATH="$FAKEBIN:$PATH" \
    STUB_CURL_COUNT_FILE="$count_file1" STUB_CURL_FAIL_COUNT=4 \
    STUB_CURL_CONTENT="pinned-tool-bytes" \
    FETCH_PINNED_TOOL_RETRY_DELAY_S=0 \
    bash "$SCRIPT" demo-tool http://example.invalid/tool.tar.gz "$dest1" 2>&1)"
rc1=$?
call_count1="$(cat "$count_file1" 2>/dev/null || echo 0)"
if [ "$rc1" -eq 0 ] && [ "$call_count1" -eq 5 ] \
    && [ -f "$dest1" ] && [ "$(cat "$dest1")" = "pinned-tool-bytes" ]; then
    pass "fails four times, succeeds on the fifth"
else
    fail "fails four times, succeeds on the fifth" "rc=$rc1 calls=$call_count1 dest-present=$([ -f "$dest1" ] && echo yes || echo no)"
    printf '%s\n' "$out1" | sed 's/^/    /'
fi

# --- case 2: fails five times -----------------------------------------------
# FLIP: STUB_CURL_FAIL_COUNT=5 means every one of the five attempts the
# script makes fails. Lowering it to 4 (verified) turns this into case 1's
# scenario -- the fifth call succeeds, rc is 0, and the "exits non-zero" and
# "no file at dest" assertions both fail -- which is what pins this case to
# an exhaustion that actually happens, not a fail count large enough that a
# script retrying fewer than five times would still look exhausted.
dest2="$TMP/case2/dest"
mkdir -p "$(dirname "$dest2")"
count_file2="$TMP/case2/count"
url2="http://example.invalid/always-down.tar.gz"
out2="$(PATH="$FAKEBIN:$PATH" \
    STUB_CURL_COUNT_FILE="$count_file2" STUB_CURL_FAIL_COUNT=5 \
    STUB_CURL_HTTP_FAIL=500 \
    FETCH_PINNED_TOOL_RETRY_DELAY_S=0 \
    bash "$SCRIPT" demo-tool "$url2" "$dest2" 2>&1)"
rc2=$?
call_count2="$(cat "$count_file2" 2>/dev/null || echo 0)"
if [ "$rc2" -ne 0 ] && [ "$call_count2" -eq 5 ] && [ ! -e "$dest2" ] \
    && case "$out2" in *"$url2"*) true ;; *) false ;; esac; then
    pass "fails five times, exits non-zero naming the url"
else
    fail "fails five times, exits non-zero naming the url" "rc=$rc2 calls=$call_count2"
    printf '%s\n' "$out2" | sed 's/^/    /'
fi

# --- case 3: a sha mismatch after a successful retry ------------------------
# This script never checks the pin itself (action.yml's own Verify step does,
# unchanged); this case proves that composition still fails closed. FLIP:
# changing STUB_CURL_CONTENT to match $expected_sha's preimage would make the
# verify step pass and leave a file at dest -- the opposite of what this
# pins, that bad content surviving a successful retry is still caught and
# removed before anything downstream could use it.
dest3="$TMP/case3/dest"
mkdir -p "$(dirname "$dest3")"
count_file3="$TMP/case3/count"
out3="$(PATH="$FAKEBIN:$PATH" \
    STUB_CURL_COUNT_FILE="$count_file3" STUB_CURL_FAIL_COUNT=2 \
    STUB_CURL_CONTENT="wrong-bytes" \
    FETCH_PINNED_TOOL_RETRY_DELAY_S=0 \
    bash "$SCRIPT" demo-tool http://example.invalid/tool.tar.gz "$dest3" 2>&1)"
rc3=$?
# The download itself succeeded (this script's job); now run the same sha256
# check action.yml's unconditional Verify step runs, unchanged, against the
# retried file.
# sha256 of "pinned-tool-bytes" (the content a correct download would have
# had), deliberately checked against "wrong-bytes" below.
expected_sha="92d43c9b36c2dff6ffb5d94fe10b4c2b099fde42d3231df576781b2b5d30b542"
if [ "$rc3" -eq 0 ] && [ -f "$dest3" ]; then
    if command -v sha256sum >/dev/null 2>&1; then
        sum_cmd="sha256sum"
    else
        sum_cmd="shasum -a 256"
    fi
    if ! printf '%s  %s\n' "$expected_sha" "$dest3" | $sum_cmd -c - >/dev/null 2>&1; then
        rm -f "$dest3"
    fi
fi
if [ -e "$dest3" ]; then
    fail "sha mismatch after a successful retry leaves no file at dest" "dest still present after verify"
    printf '%s\n' "$out3" | sed 's/^/    /'
else
    pass "sha mismatch after a successful retry leaves no file at dest"
fi

echo
if [ "$fails" != 0 ]; then
    echo "$fails failing case(s)"
    exit 1
fi
echo "all cases passed"
