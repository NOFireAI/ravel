#!/usr/bin/env bash
# Decision logic of the released-binary fence drill (scripts/compat/drill-lib.sh)
# on recorded key lists: no network, no RustFS, no ravel binary.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=scripts/compat/drill-lib.sh
. "$HERE/drill-lib.sh"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0
ok() {
  pass=$((pass + 1))
  echo "ok   $1"
}
bad() {
  fail=$((fail + 1))
  echo "FAIL $1"
}

H=85677b485d597b8963de41da73951a4d
OTHER=00000000000000000000000000000001
FENCE=$(date -u -d 2026-11-16T19:40:18Z +%s)
HORIZON=$(drill_ingest_horizon "$FENCE" 60)
ALERTS=$((FENCE + 63))

# Horizon: (hour of the fence + 3) hours, plus the request margin.
[ "$HORIZON" -eq "$(date -u -d 2026-11-16T22:01:00Z +%s)" ] && ok "horizon of a 19:40:18 fence is 22:01:00" || bad "horizon 19:40:18 gave $HORIZON"
h2=$(drill_ingest_horizon "$(date -u -d 2026-11-16T20:00:00Z +%s)" 0)
[ "$h2" -eq "$(date -u -d 2026-11-16T23:00:00Z +%s)" ] && ok "horizon of a 20:00:00 fence is 23:00:00" || bad "horizon 20:00:00 gave $h2"
h3=$(drill_ingest_horizon "$(date -u -d 2026-11-16T19:59:59Z +%s)" 0)
[ "$h3" -eq "$(date -u -d 2026-11-16T22:00:00Z +%s)" ] && ok "horizon of a 19:59:59 fence is 22:00:00" || bad "horizon 19:59:59 gave $h3"

# Classification.
for case in \
  "t/$H/m/l0/0000/x.rseg fenced" \
  "t/$H/l/c/0003/20261116T19/x.cmt fenced" \
  "t/$H/s/admission/p.snapshot fenced" \
  "t/$H/m/prov fenced" \
  "t/$H/catalog/m/HEAD fenced" \
  "t/$H/catalog/s/snap/x.csnap fenced" \
  "t/$H/a/alert-lease alerts" \
  "t/$H/a/state/latest alerts" \
  "t/$H/u/l0/0001/x.rseg audit" \
  "t/$H/config tenant" \
  "t/$OTHER/m/l0/0000/x.rseg other" \
  "sys/maintain/memo/x other"; do
  set -- $case
  got=$(drill_classify "$1" "$H")
  [ "$got" = "$2" ] && ok "classify $1 = $2" || bad "classify $1 = $got, want $2"
done

# Recorded listings: the fenced listing, then an after listing with
#   - a new metrics object inside the horizon (allowed),
#   - an audit object after the horizon (audit is not fenced),
#   - an overwrite of the provisioning record's neighbour with a new time,
#   - an untouched key.
printf '%s\t%s\t%s\n' \
  "t/$H/m/prov" 2026-11-16T19:40:18.635Z 120 \
  "t/$H/m/l0/0000/a.rseg" 2026-11-16T19:39:00.000Z 900 \
  "t/$H/l/admission/p.snapshot" 2026-11-16T19:40:10.000Z 80 \
  "t/$H/a/alert-lease" 2026-11-16T19:40:18.900Z 100 \
  > "$TMP/fenced"
cp "$TMP/fenced" "$TMP/after-pass"
printf '%s\t%s\t%s\n' \
  "t/$H/m/l0/0000/b.rseg" 2026-11-16T21:30:00.000Z 900 \
  "t/$H/u/l0/0001/c.rseg" 2026-11-16T23:30:00.000Z 400 \
  "t/$OTHER/m/l0/0000/d.rseg" 2026-11-16T23:30:00.000Z 400 \
  >> "$TMP/after-pass"
drill_written_keys "$TMP/fenced" "$TMP/after-pass" > "$TMP/w-pass"
[ "$(grep -c . "$TMP/w-pass")" -eq 3 ] && ok "three keys written in the pass case" || bad "written count $(grep -c . "$TMP/w-pass")"
if drill_late_writes "$TMP/w-pass" "$H" "$HORIZON" "$ALERTS" > "$TMP/late-pass"; then
  ok "in-horizon fenced write, late audit and other tenant pass"
else
  bad "pass case reported late writes: $(cat "$TMP/late-pass")"
fi

# Fail: a fenced object one second past the horizon, and an overwritten
# admission snapshot (same key, new LastModified) past it.
cp "$TMP/fenced" "$TMP/after-fail"
sed -i "s#^t/$H/l/admission/p.snapshot\t[^\t]*#t/$H/l/admission/p.snapshot\t2026-11-16T22:05:00.000Z#" "$TMP/after-fail"
printf '%s\t%s\t%s\n' "t/$H/s/c/0000/20261116T21/x.cmt" 2026-11-16T22:01:01.000Z 300 >> "$TMP/after-fail"
drill_written_keys "$TMP/fenced" "$TMP/after-fail" > "$TMP/w-fail"
if drill_late_writes "$TMP/w-fail" "$H" "$HORIZON" "$ALERTS" > "$TMP/late-fail"; then
  bad "post-horizon fenced writes were not reported"
else
  ok "post-horizon fenced writes fail the drill"
fi
grep -qF "late fenced t/$H/s/c/0000/20261116T21/x.cmt 2026-11-16T22:01:01.000Z" "$TMP/late-fail" \
  && ok "late commit record named with its LastModified" || bad "late commit record missing: $(cat "$TMP/late-fail")"
grep -qF "late fenced t/$H/l/admission/p.snapshot 2026-11-16T22:05:00.000Z" "$TMP/late-fail" \
  && ok "overwrite past the horizon is a late write" || bad "overwrite missing: $(cat "$TMP/late-fail")"
[ "$(grep -c . "$TMP/late-fail")" -eq 2 ] && ok "exactly two late writes" || bad "late count $(grep -c . "$TMP/late-fail")"

# Exactly at the horizon second is still inside it.
printf '%s\t%s\t%s\n' "t/$H/m/l0/0000/edge.rseg" 2026-11-16T22:01:00.999Z 1 > "$TMP/w-edge"
drill_late_writes "$TMP/w-edge" "$H" "$HORIZON" "$ALERTS" > /dev/null && ok "write within the horizon second passes" || bad "horizon second reported late"

# Alerts: the parked lease rewritten after the alert horizon fails.
printf '%s\t%s\t%s\n' "t/$H/a/alert-lease" 2026-11-16T19:45:00.000Z 100 > "$TMP/w-alert"
if drill_late_writes "$TMP/w-alert" "$H" "$HORIZON" "$ALERTS" > "$TMP/late-alert"; then
  bad "lease rewrite after the alert horizon passed"
else
  ok "lease rewrite after the alert horizon fails"
fi

# Removed keys.
drill_removed_keys "$TMP/after-pass" "$TMP/fenced" > "$TMP/removed"
[ "$(grep -c . "$TMP/removed")" -eq 3 ] && ok "removed keys found" || bad "removed count $(grep -c . "$TMP/removed")"

# Positive control: every prefix nonzero passes.
: > "$TMP/pc-listing"
for p in $PC_PREFIXES; do
  printf '%s\t%s\t%s\n' "t/$H/$p/k" 2026-11-16T19:39:00.000Z 1 >> "$TMP/pc-listing"
done
: > "$TMP/pc-report"
positive_control "$TMP/pc-listing" "$H" "$TMP/pc-report" > /dev/null && ok "positive control with every prefix passes" || bad "full positive control failed"

# A zero positive control fails: drop the audit commits.
grep -v "/u/c/" "$TMP/pc-listing" > "$TMP/pc-zero"
: > "$TMP/pc-report"
if positive_control "$TMP/pc-zero" "$H" "$TMP/pc-report" > "$TMP/pc-out"; then
  bad "zero positive control passed"
else
  ok "zero positive control fails"
fi
grep -qF "band-miss pc_u_c=0 outside [1, 1000000]" "$TMP/pc-out" && ok "zero figure named" || bad "zero figure not named: $(cat "$TMP/pc-out")"

# An empty listing fails every figure.
: > "$TMP/pc-report"
positive_control /dev/null "$H" "$TMP/pc-report" > "$TMP/pc-empty" && bad "empty listing passed" || ok "empty listing fails"

# Figures: missing, duplicated and non-integer all fail.
printf 'figure a 3\nfigure b 1\nfigure b 1\nfigure c x\n' > "$TMP/fig"
drill_check_figure "$TMP/fig" a 1 5 > /dev/null && ok "figure in band passes" || bad "figure a failed"
drill_check_figure "$TMP/fig" a 4 5 > /dev/null && bad "figure out of band passed" || ok "figure out of band fails"
drill_check_figure "$TMP/fig" b 0 5 > /dev/null && bad "duplicated figure passed" || ok "duplicated figure fails"
drill_check_figure "$TMP/fig" c 0 5 > /dev/null && bad "non-integer figure passed" || ok "non-integer figure fails"
drill_check_figure "$TMP/fig" missing 0 5 > /dev/null && bad "missing figure passed" || ok "missing figure fails"

echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
