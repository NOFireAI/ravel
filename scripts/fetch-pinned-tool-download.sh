#!/usr/bin/env bash
# fetch-pinned-tool-download.sh <name> <url> <dest>
#
# Downloads <url> to <dest> for fetch-pinned-tool/action.yml's cache-miss
# step. Up to five attempts, exponential backoff between them (default 10,
# 20, 40, 80 s), all inside a wall-clock budget (default 240 s): no attempt
# runs past the budget, and no retry starts once its backoff would reach it.
# The actionlint job fetches two tools inside a 10-minute timeout, so two
# spent budgets still leave it time to run. FETCH_PINNED_TOOL_RETRY_DELAY_S
# overrides the base delay and FETCH_PINNED_TOOL_BUDGET_S the budget. A
# short upstream outage (issue #4093: a merge-group run missed an evicted
# tool cache, then got HTTP 500 on all three of the previous
# three-attempt/15s-backoff loop's tries) is the failure this widens the
# window for.
#
# On final failure, removes any partial file and exits 1 naming the url, the
# attempts made, the budget, and the last HTTP status curl reported. Leaves
# the sha256 verify this step's caller runs afterward untouched: this script
# only downloads, it never checks the pin.
set -u

name="${1:?usage: fetch-pinned-tool-download.sh <name> <url> <dest>}"
url="${2:?usage: fetch-pinned-tool-download.sh <name> <url> <dest>}"
dest="${3:?usage: fetch-pinned-tool-download.sh <name> <url> <dest>}"

base_delay="${FETCH_PINNED_TOOL_RETRY_DELAY_S:-10}"
budget="${FETCH_PINNED_TOOL_BUDGET_S:-240}"
max_attempts=5
attempt_cap_s=120
tmp="${dest}.download"

mkdir -p "$(dirname "$dest")"
rm -f "$tmp"

deadline=$(($(date +%s) + budget))
last_status="unknown"
last_rc=0
made=0
ok=0
while [ "$made" -lt "$max_attempts" ]; do
    remaining=$((deadline - $(date +%s)))
    if [ "$remaining" -le 0 ]; then
        break
    fi
    max_time=$attempt_cap_s
    if [ "$remaining" -lt "$max_time" ]; then
        max_time=$remaining
    fi
    made=$((made + 1))
    echo "fetch-pinned-tool: downloading $name from $url (attempt $made/$max_attempts, ${max_time}s cap)"
    last_status="$(curl -fsSL --connect-timeout 20 --max-time "$max_time" -o "$tmp" -w '%{http_code}' "$url")"
    last_rc=$?
    if [ "$last_rc" -eq 0 ]; then
        ok=1
        break
    fi
    rm -f "$tmp"
    if [ "$made" -lt "$max_attempts" ]; then
        delay=$((base_delay * (2 ** (made - 1))))
        if [ $(($(date +%s) + delay)) -ge "$deadline" ]; then
            echo "fetch-pinned-tool: download failed (http ${last_status:-unknown}); a ${delay}s backoff would reach the ${budget}s budget, giving up"
            break
        fi
        echo "fetch-pinned-tool: download failed (http ${last_status:-unknown}), retrying in ${delay}s"
        sleep "$delay"
    fi
done

if [ "$ok" -ne 1 ]; then
    echo "::error::fetch-pinned-tool: download of $name from $url failed after $made attempts within a ${budget}s budget (last http status: ${last_status:-unknown}, curl exit ${last_rc})" >&2
    exit 1
fi

mv "$tmp" "$dest"
