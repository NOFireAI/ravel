#!/usr/bin/env bash
# Cases for fetch-pinned-tool-download.sh (issue #4093). A stubbed curl on
# PATH stands in for the real download: it counts its own invocations in
# STUB_CURL_COUNT_FILE, fails STUB_CURL_FAIL_COUNT times, writing
# STUB_CURL_PARTIAL_CONTENT to the -o destination (mirroring a real curl
# leaving a partial file behind on a failed transfer) and STUB_CURL_HTTP_FAIL
# on stdout with a non-zero exit, then succeeds, writing STUB_CURL_CONTENT to
# the -o destination and STUB_CURL_HTTP_OK on stdout. A stubbed sleep on PATH
# logs its argument to STUB_SLEEP_LOG_FILE instead of sleeping, so the
# backoff schedule can be asserted without the suite paying for it.
#
# FETCH_PINNED_TOOL_RETRY_DELAY_S=0 in every case but the backoff-math one:
# those cases exercise the retry COUNT, not the sleep duration, and a test
# that slept the real 10/20/40/80 s schedule would cost the suite minutes for
# no added coverage.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/fetch-pinned-tool-download.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
pass() { echo "ok   $1"; }
fail() { echo "FAIL $1: $2"; fails=$((fails + 1)); }
contains() { case "$1" in *"$2"*) return 0 ;; *) return 1 ;; esac; }

# A fake PATH entry providing only `curl` and `sleep`, so the production
# script's own calls to both resolve to the stubs, not the real binaries.
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
    printf '%s' "${STUB_CURL_PARTIAL_CONTENT:-partial-bytes}" >"$out"
    printf '%s' "${STUB_CURL_HTTP_FAIL:-500}"
    exit 22
fi
printf '%s' "${STUB_CURL_CONTENT:-ok}" >"$out"
printf '%s' "${STUB_CURL_HTTP_OK:-200}"
exit 0
STUB
chmod +x "$FAKEBIN/curl"

cat >"$FAKEBIN/sleep" <<'STUB'
#!/usr/bin/env bash
set -u
if [ -n "${STUB_SLEEP_LOG_FILE:-}" ]; then
    echo "$1" >>"$STUB_SLEEP_LOG_FILE"
fi
exit 0
STUB
chmod +x "$FAKEBIN/sleep"

# --- case 1: fails four times and succeeds on the fifth --------------------
# FLIP: STUB_CURL_FAIL_COUNT=4 means the fifth curl call is the first success.
# Lowering it to 3 (success on the fourth call) would let this case pass with
# only four calls recorded -- the opposite of what it is meant to pin, that
# the script actually drives all five attempts before giving up on four
# failures. Asserting call_count -eq 5 is what catches that flip. The tmp-file
# assertion pins that a successful run leaves no partial-download file beside
# the real one.
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
    && [ -f "$dest1" ] && [ "$(cat "$dest1")" = "pinned-tool-bytes" ] \
    && [ ! -e "$dest1.download" ]; then
    pass "fails four times, succeeds on the fifth, no tmp file left behind"
else
    fail "fails four times, succeeds on the fifth, no tmp file left behind" \
        "rc=$rc1 calls=$call_count1 dest-present=$([ -f "$dest1" ] && echo yes || echo no) tmp-present=$([ -e "$dest1.download" ] && echo yes || echo no)"
    printf '%s\n' "$out1" | sed 's/^/    /'
fi

# --- case 2: fails five times -----------------------------------------------
# FLIP: STUB_CURL_FAIL_COUNT=5 means every one of the five attempts the
# script makes fails. Lowering it to 4 (verified) turns this into case 1's
# scenario -- the fifth call succeeds, rc is 0, and the "exits non-zero" and
# "no file at dest" assertions both fail -- which is what pins this case to
# an exhaustion that actually happens, not a fail count large enough that a
# script retrying fewer than five times would still look exhausted. The
# message assertions pin that the error names the attempt count and the last
# status curl reported, not just the url; the tmp-file assertion pins that an
# exhausted run leaves no partial file behind either.
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
if [ "$rc2" -ne 0 ] && [ "$call_count2" -eq 5 ] \
    && [ ! -e "$dest2" ] && [ ! -e "$dest2.download" ] \
    && contains "$out2" "$url2" \
    && contains "$out2" "after 5 attempts" \
    && contains "$out2" "500"; then
    pass "fails five times, exits non-zero naming the url, attempt count, last status, no tmp file left"
else
    fail "fails five times, exits non-zero naming the url, attempt count, last status, no tmp file left" \
        "rc=$rc2 calls=$call_count2 tmp-present=$([ -e "$dest2.download" ] && echo yes || echo no)"
    printf '%s\n' "$out2" | sed 's/^/    /'
fi

# --- case 3: a sha mismatch after a successful retry ------------------------
# This script never checks the pin itself (action.yml's own Verify step does,
# unchanged); this case proves that composition still fails closed. The
# pre-check that the script exited 0 with dest present pins that the verify
# below actually runs on a real downloaded file: without it, a mutant that
# replaces the final `mv` with `exit 1` would leave the script failing with
# no file at dest, and the loose version of this case (verify only when
# rc -eq 0 and dest exists, pass otherwise) would call that a pass too, since
# "no file at dest" also describes an exhausted run. FLIP: changing
# STUB_CURL_CONTENT to match $expected_sha's preimage would make the verify
# step pass and leave a file at dest -- the opposite of what this pins, that
# bad content surviving a successful retry is still caught and removed before
# anything downstream could use it.
dest3="$TMP/case3/dest"
mkdir -p "$(dirname "$dest3")"
count_file3="$TMP/case3/count"
out3="$(PATH="$FAKEBIN:$PATH" \
    STUB_CURL_COUNT_FILE="$count_file3" STUB_CURL_FAIL_COUNT=2 \
    STUB_CURL_CONTENT="wrong-bytes" \
    FETCH_PINNED_TOOL_RETRY_DELAY_S=0 \
    bash "$SCRIPT" demo-tool http://example.invalid/tool.tar.gz "$dest3" 2>&1)"
rc3=$?
if [ "$rc3" -ne 0 ] || [ ! -f "$dest3" ]; then
    fail "sha mismatch after a successful retry leaves no file at dest" \
        "script did not succeed before verify: rc=$rc3 dest-present=$([ -f "$dest3" ] && echo yes || echo no)"
    printf '%s\n' "$out3" | sed 's/^/    /'
else
    # The download itself succeeded (this script's job); now run the same
    # sha256 check action.yml's unconditional Verify step runs, unchanged,
    # against the retried file.
    # sha256 of "pinned-tool-bytes" (the content a correct download would
    # have had), deliberately checked against "wrong-bytes" below.
    expected_sha="92d43c9b36c2dff6ffb5d94fe10b4c2b099fde42d3231df576781b2b5d30b542"
    if command -v sha256sum >/dev/null 2>&1; then
        sum_cmd="sha256sum"
    else
        sum_cmd="shasum -a 256"
    fi
    if ! printf '%s  %s\n' "$expected_sha" "$dest3" | $sum_cmd -c - >/dev/null 2>&1; then
        rm -f "$dest3"
    fi
    if [ -e "$dest3" ]; then
        fail "sha mismatch after a successful retry leaves no file at dest" "dest still present after verify"
        printf '%s\n' "$out3" | sed 's/^/    /'
    else
        pass "sha mismatch after a successful retry leaves no file at dest"
    fi
fi

# --- case 4: backoff doubles 1 2 4 8 across five attempts -------------------
# FLIP: a mutant that replaces the computed `delay` with a constant
# (always base_delay) produces the sleep log "1 1 1 1" instead of
# "1 2 4 8", and this assertion is what catches it; the earlier cases all
# set FETCH_PINNED_TOOL_RETRY_DELAY_S=0, where every delay is 0 and
# doubling is invisible.
dest4="$TMP/case4/dest"
mkdir -p "$(dirname "$dest4")"
count_file4="$TMP/case4/count"
sleep_log4="$TMP/case4/sleep.log"
: >"$sleep_log4"
out4="$(PATH="$FAKEBIN:$PATH" \
    STUB_CURL_COUNT_FILE="$count_file4" STUB_CURL_FAIL_COUNT=5 \
    STUB_CURL_HTTP_FAIL=500 \
    STUB_SLEEP_LOG_FILE="$sleep_log4" \
    FETCH_PINNED_TOOL_RETRY_DELAY_S=1 \
    bash "$SCRIPT" demo-tool http://example.invalid/always-down.tar.gz "$dest4" 2>&1)"
rc4=$?
sleeps4="$(tr '\n' ' ' <"$sleep_log4" | sed 's/[[:space:]]*$//')"
if [ "$rc4" -ne 0 ] && [ "$sleeps4" = "1 2 4 8" ]; then
    pass "backoff doubles 1 2 4 8 across five attempts"
else
    fail "backoff doubles 1 2 4 8 across five attempts" "rc=$rc4 sleeps=[$sleeps4]"
    printf '%s\n' "$out4" | sed 's/^/    /'
fi

echo
if [ "$fails" != 0 ]; then
    echo "$fails failing case(s)"
    exit 1
fi
echo "all cases passed"
