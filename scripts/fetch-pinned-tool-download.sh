#!/usr/bin/env bash
# fetch-pinned-tool-download.sh <name> <url> <dest>
#
# Downloads <url> to <dest> for fetch-pinned-tool/action.yml's cache-miss
# step. Five attempts, exponential backoff between them (default 10, 20, 40,
# 80 s, about 2.5 min total); FETCH_PINNED_TOOL_RETRY_DELAY_S overrides the
# base delay so a test does not sleep. A short upstream outage (issue #4093:
# a merge-group run missed an evicted tool cache, then got HTTP 500 on all
# three of the previous three-attempt/15s-backoff loop's tries) is the
# failure this widens the window for.
#
# On final failure, removes any partial file and exits 1 naming the url, the
# attempt count, and the last HTTP status curl reported. Leaves the sha256
# verify this step's caller runs afterward untouched: this script only
# downloads, it never checks the pin.
set -u

name="${1:?usage: fetch-pinned-tool-download.sh <name> <url> <dest>}"
url="${2:?usage: fetch-pinned-tool-download.sh <name> <url> <dest>}"
dest="${3:?usage: fetch-pinned-tool-download.sh <name> <url> <dest>}"

base_delay="${FETCH_PINNED_TOOL_RETRY_DELAY_S:-10}"
max_attempts=5
tmp="${dest}.download"

mkdir -p "$(dirname "$dest")"
rm -f "$tmp"

last_status="unknown"
last_rc=0
attempt=1
ok=0
while [ "$attempt" -le "$max_attempts" ]; do
    echo "fetch-pinned-tool: downloading $name from $url (attempt $attempt/$max_attempts)"
    last_status="$(curl -fsSL --connect-timeout 20 --max-time 120 -o "$tmp" -w '%{http_code}' "$url")"
    last_rc=$?
    if [ "$last_rc" -eq 0 ]; then
        ok=1
        break
    fi
    rm -f "$tmp"
    if [ "$attempt" -lt "$max_attempts" ]; then
        delay=$((base_delay * (2 ** (attempt - 1))))
        echo "fetch-pinned-tool: download failed (http ${last_status:-unknown}), retrying in ${delay}s"
        sleep "$delay"
    fi
    attempt=$((attempt + 1))
done

if [ "$ok" -ne 1 ]; then
    echo "::error::fetch-pinned-tool: download of $name from $url failed after $max_attempts attempts (last http status: ${last_status:-unknown}, curl exit ${last_rc})" >&2
    exit 1
fi

mv "$tmp" "$dest"
