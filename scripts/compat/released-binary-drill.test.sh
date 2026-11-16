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
  && ok "overwrite past the horizon is a late write with no exempt list" || bad "overwrite missing: $(cat "$TMP/late-fail")"
[ "$(grep -c . "$TMP/late-fail")" -eq 2 ] && ok "exactly two late writes" || bad "late count $(grep -c . "$TMP/late-fail")"

# The shipped exempt list, read from the drill itself.
eval "$(grep '^EXEMPT_PATTERNS=' "$HERE/released-binary-drill.sh")"
[ -n "${EXEMPT_PATTERNS:-}" ] && ok "drill defines EXEMPT_PATTERNS" || bad "no EXEMPT_PATTERNS in the drill"
for case in \
  "t/$H/m/admission/0f3c.snapshot yes" \
  "t/$H/l/admission/0f3c.snapshot yes" \
  "t/$H/s/admission/0f3c.snapshot yes" \
  "t/$H/m/maint/scrub/0000.cursor yes" \
  "t/$H/l/maint/scrub/0003.cursor yes" \
  "t/$H/s/maint/scrub/0001.cursor yes" \
  "t/$H/m/admission/x/0f3c.snapshot no" \
  "t/$H/m/maint/scrub/x/0000.cursor no" \
  "t/$H/m/maint/0001/cursor no" \
  "t/$H/m/meta no" \
  "t/$H/m/prov no" \
  "t/$H/m/l0/0000/w.1.00000000000000000001.0011223344556677.rseg no" \
  "t/$H/m/c/0000/20261116T22/w.1.00000000000000000001.cmt no" \
  "t/$H/l/idem/k.20261116T22.idm no" \
  "t/$H/catalog/m/HEAD no" \
  "t/$H/a/alert-lease no" \
  "t/$H/a/state/latest no" \
  "t/$H/u/maint/0001/cursor no" \
  "t/$OTHER/m/admission/0f3c.snapshot no"; do
  set -- $case
  if drill_exempt "$1" "$H" "$EXEMPT_PATTERNS"; then got=yes; else got=no; fi
  [ "$got" = "$2" ] && ok "exempt $1 = $2" || bad "exempt $1 = $got, want $2"
done

# Narrowed rule: with the shipped list, the late admission overwrite is an
# exempt write recorded with its LastModified, and the late commit record
# still fails.
if drill_late_writes "$TMP/w-fail" "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > "$TMP/ev-fail"; then
  bad "non-exempt post-horizon commit record passed under the exempt list"
else
  ok "non-exempt post-horizon commit record fails under the exempt list"
fi
[ "$(grep -c '^late ' "$TMP/ev-fail")" -eq 1 ] && grep -qF "late fenced t/$H/s/c/0000/20261116T21/x.cmt 2026-11-16T22:01:01.000Z" "$TMP/ev-fail" \
  && ok "exactly the commit record is late" || bad "late lines: $(grep '^late ' "$TMP/ev-fail")"
grep -qxF "exempt fenced t/$H/l/admission/p.snapshot 2026-11-16T22:05:00.000Z after" "$TMP/ev-fail" \
  && ok "exempt write recorded with LastModified, after the horizon" || bad "exempt line: $(cat "$TMP/ev-fail")"

# An exempt key alone, past the horizon, passes; an exempt key inside the
# horizon is recorded as within.
printf '%s\t%s\t%s\n' \
  "t/$H/m/maint/scrub/0000.cursor" 2026-11-16T22:40:00.000Z 200 \
  "t/$H/s/admission/q.snapshot" 2026-11-16T20:00:00.000Z 80 > "$TMP/w-exempt"
if drill_late_writes "$TMP/w-exempt" "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > "$TMP/ev-exempt"; then
  ok "exempt keys past the horizon pass"
else
  bad "exempt keys failed: $(cat "$TMP/ev-exempt")"
fi
grep -qxF "exempt fenced t/$H/m/maint/scrub/0000.cursor 2026-11-16T22:40:00.000Z after" "$TMP/ev-exempt" \
  && grep -qxF "exempt fenced t/$H/s/admission/q.snapshot 2026-11-16T20:00:00.000Z within" "$TMP/ev-exempt" \
  && ok "exempt keys recorded after and within" || bad "exempt records: $(cat "$TMP/ev-exempt")"

# A non-exempt advisory-looking key past the horizon fails: m/meta and an
# extra-segment admission key are not on the list.
printf '%s\t%s\t%s\n' \
  "t/$H/m/meta" 2026-11-16T22:40:00.000Z 300 \
  "t/$H/m/admission/x/q.snapshot" 2026-11-16T22:40:00.000Z 80 > "$TMP/w-nonexempt"
if drill_late_writes "$TMP/w-nonexempt" "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > "$TMP/ev-nonexempt"; then
  bad "non-exempt keys passed: $(cat "$TMP/ev-nonexempt")"
else
  ok "non-exempt keys past the horizon fail"
fi
[ "$(grep -c '^late ' "$TMP/ev-nonexempt")" -eq 2 ] && ok "both non-exempt keys late" || bad "late lines: $(cat "$TMP/ev-nonexempt")"

# The parked lease is not exempt: a rewrite after the alert horizon fails
# with the shipped list too.
printf '%s\t%s\t%s\n' "t/$H/a/alert-lease" 2026-11-16T19:45:00.000Z 100 > "$TMP/w-alert2"
drill_late_writes "$TMP/w-alert2" "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > /dev/null \
  && bad "lease rewrite passed under the exempt list" || ok "lease rewrite fails under the exempt list"

# Trailer parsing: version is the u16 LE at bytes 8..10, magic the last 4.
printf 'payload\x05\x00\x00\x00\xaa\xbb\xcc\xdd\x07\x00\x01\x00RSG1' > "$TMP/obj"
tl=$(python3 -I "$HERE/drill_s3.py" trailer-file "$TMP/obj" | cut -f2-)
[ "$tl" = "RSG1	7	1" ] && ok "trailer-file reads RSG1 version 7 signal 1" || bad "trailer-file gave '$tl'"
printf '\x05\x00\x00\x00\xaa\xbb\xcc\xdd\x05\x01\x02\x00RLG1' > "$TMP/obj2"
tl=$(python3 -I "$HERE/drill_s3.py" trailer-file "$TMP/obj2" | cut -f2-)
[ "$tl" = "RLG1	261	2" ] && ok "trailer version is little-endian u16" || bad "trailer-file gave '$tl'"
printf 'short' > "$TMP/obj3"
python3 -I "$HERE/drill_s3.py" trailer-file "$TMP/obj3" > /dev/null 2>&1 && bad "short object read as a trailer" || ok "short object refused"

# Teardown: the writes between the running and the post-stop listing.
DM="t/$H/m/l0/0000/w.1.00000000000000000007.0011223344556677.rseg"
CM="t/$H/m/c/0000/20261116T22/w.1.00000000000000000007.cmt"
DL="t/$H/l/l0/0001/w.1.00000000000000000003.8899aabbccddeeff.rseg"
CL="t/$H/l/c/0001/20261116T22/w.1.00000000000000000003.cmt"
DS="t/$H/s/l0/0000/w.1.00000000000000000002.0102030405060708.rseg"
AD="t/$H/m/admission/0f3c.snapshot"
printf '%s\t%s\t%s\n' \
  "$DM" 2026-11-16T23:02:39.000Z 900 \
  "$CM" 2026-11-16T23:02:39.500Z 300 \
  "$DL" 2026-11-16T23:02:40.000Z 900 \
  "$CL" 2026-11-16T23:02:40.500Z 300 \
  "$DS" 2026-11-16T23:02:41.000Z 900 \
  "$AD" 2026-11-16T23:02:38.000Z 80 \
  "t/$H/u/l0/0001/audit.rseg" 2026-11-16T23:02:41.000Z 400 > "$TMP/td-written"
cp "$TMP/td-written" "$TMP/td-listing"
printf '%s\t%s\t%s\n' "$DM" RSG1 7 "$DL" RLG1 5 "$DS" RSP1 5 > "$TMP/td-trailers"
if drill_teardown_check "$TMP/td-written" "$H" "$TMP/td-listing" "$TMP/td-trailers" "$EXEMPT_PATTERNS" > "$TMP/td-ok"; then
  ok "teardown writes at the release versions pass"
else
  bad "teardown at release versions failed: $(cat "$TMP/td-ok")"
fi
[ "$(grep -c '^teardown-ok ' "$TMP/td-ok")" -eq 5 ] && ok "five teardown keys checked" || bad "teardown-ok count: $(cat "$TMP/td-ok")"
grep -qxF "teardown-ok commit $CM 2026-11-16T23:02:39.500Z data=$DM RSG1 7" "$TMP/td-ok" \
  && ok "commit record checked through its data object" || bad "commit line: $(cat "$TMP/td-ok")"
grep -qxF "teardown-exempt fenced $AD 2026-11-16T23:02:38.000Z" "$TMP/td-ok" \
  && ok "exempt teardown write recorded" || bad "exempt teardown line: $(cat "$TMP/td-ok")"
grep -q "u/l0" "$TMP/td-ok" && bad "audit key judged at teardown" || ok "audit key not judged at teardown"

# Wrong version: metrics at RSEG 8, spans at RSPAN 4, logs with RSEG's magic.
for wrong in "$DM RSG1 8" "$DS RSP1 4" "$DL RSG1 5"; do
  set -- $wrong
  awk -F'\t' -v k="$1" -v m="$2" -v v="$3" 'BEGIN { OFS = "\t" } $1 == k { $2 = m; $3 = v } { print }' \
    "$TMP/td-trailers" > "$TMP/td-wrong"
  if drill_teardown_check "$TMP/td-written" "$H" "$TMP/td-listing" "$TMP/td-wrong" "$EXEMPT_PATTERNS" > "$TMP/td-bad"; then
    bad "teardown write at $2 $3 passed"
  else
    ok "teardown write at $2 $3 fails"
  fi
done
# The commit record inherits its data object's failure.
awk -F'\t' -v k="$DM" 'BEGIN { OFS = "\t" } $1 == k { $3 = 8 } { print }' "$TMP/td-trailers" > "$TMP/td-wrong"
drill_teardown_check "$TMP/td-written" "$H" "$TMP/td-listing" "$TMP/td-wrong" "$EXEMPT_PATTERNS" > "$TMP/td-bad" || true
[ "$(grep -c '^teardown-bad version ' "$TMP/td-bad")" -eq 2 ] && ok "data object and its commit record both fail at RSEG 8" \
  || bad "wrong-version lines: $(cat "$TMP/td-bad")"

# A missing trailer, a commit record with no data object, and a teardown key
# no version rule covers each fail.
grep -v "^$DS" "$TMP/td-trailers" > "$TMP/td-missing"
drill_teardown_check "$TMP/td-written" "$H" "$TMP/td-listing" "$TMP/td-missing" "$EXEMPT_PATTERNS" > /dev/null \
  && bad "teardown data object with no trailer passed" || ok "teardown data object with no trailer fails"
grep -v "^$DL" "$TMP/td-listing" > "$TMP/td-nodata"
printf '%s\t%s\t%s\n' "$CL" 2026-11-16T23:02:40.500Z 300 > "$TMP/td-w-commit"
drill_teardown_check "$TMP/td-w-commit" "$H" "$TMP/td-nodata" "$TMP/td-trailers" "$EXEMPT_PATTERNS" > "$TMP/td-c" \
  && bad "unpaired teardown commit record passed" || ok "unpaired teardown commit record fails"
grep -qxF "teardown-bad commit $CL 2026-11-16T23:02:40.500Z no single paired data object" "$TMP/td-c" \
  && ok "commit record with no data object fails" || bad "unpaired commit: $(cat "$TMP/td-c")"
printf '%s\t%s\t%s\n' "t/$H/m/meta" 2026-11-16T23:02:41.000Z 300 > "$TMP/td-w-meta"
drill_teardown_check "$TMP/td-w-meta" "$H" "$TMP/td-listing" "$TMP/td-trailers" "$EXEMPT_PATTERNS" > "$TMP/td-m" \
  && bad "teardown m/meta passed" || ok "teardown key with no version rule fails"
grep -qxF "teardown-bad other t/$H/m/meta 2026-11-16T23:02:41.000Z no version rule covers this key" "$TMP/td-m" \
  && ok "unversioned teardown key named" || bad "meta line: $(cat "$TMP/td-m")"

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
drill_check_figure "$TMP/fig" b 0 5 > "$TMP/fig-b" && bad "duplicated figure passed" || ok "duplicated figure fails"
grep -qF "band-miss b present 2 times (want exactly 1)" "$TMP/fig-b" && ok "duplicated figure named as such" || bad "duplicate reason: $(cat "$TMP/fig-b")"
drill_check_figure "$TMP/fig" c 0 5 > /dev/null && bad "non-integer figure passed" || ok "non-integer figure fails"
drill_check_figure "$TMP/fig" missing 0 5 > /dev/null && bad "missing figure passed" || ok "missing figure fails"

echo "passed=$pass failed=$fail"
[ "$fail" -eq 0 ]
