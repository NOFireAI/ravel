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
TRUST=$(drill_trust_limit "$FENCE")

# Horizon: (hour of the fence + 3) hours, plus the request margin.
[ "$HORIZON" -eq "$(date -u -d 2026-11-16T22:01:00Z +%s)" ] && ok "horizon of a 19:40:18 fence is 22:01:00" || bad "horizon 19:40:18 gave $HORIZON"
h2=$(drill_ingest_horizon "$(date -u -d 2026-11-16T20:00:00Z +%s)" 0)
[ "$h2" -eq "$(date -u -d 2026-11-16T23:00:00Z +%s)" ] && ok "horizon of a 20:00:00 fence is 23:00:00" || bad "horizon 20:00:00 gave $h2"
h3=$(drill_ingest_horizon "$(date -u -d 2026-11-16T19:59:59Z +%s)" 0)
[ "$h3" -eq "$(date -u -d 2026-11-16T22:00:00Z +%s)" ] && ok "horizon of a 19:59:59 fence is 22:00:00" || bad "horizon 19:59:59 gave $h3"

# Trust limit: (hour of the fence + 2) hours, no margin.
[ "$TRUST" -eq "$(date -u -d 2026-11-16T21:00:00Z +%s)" ] && ok "trust limit of a 19:40:18 fence is 21:00:00" || bad "trust 19:40:18 gave $TRUST"
t2=$(drill_trust_limit "$(date -u -d 2026-11-16T20:00:00Z +%s)")
[ "$t2" -eq "$(date -u -d 2026-11-16T22:00:00Z +%s)" ] && ok "trust limit of a 20:00:00 fence is 22:00:00" || bad "trust 20:00:00 gave $t2"
t3=$(drill_trust_limit "$(date -u -d 2026-11-16T19:59:59Z +%s)")
[ "$t3" -eq "$(date -u -d 2026-11-16T21:00:00Z +%s)" ] && ok "trust limit of a 19:59:59 fence is 21:00:00" || bad "trust 19:59:59 gave $t3"

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

# Post-fence events over a listing chain. The fence listing, a snapshot
# while running, the running listing after the driver stopped, and the
# listing after the processes stopped.
printf '%s\t%s\t%s\n' \
  "t/$H/m/prov" 2026-11-16T19:40:18.635Z 120 \
  "t/$H/m/l0/0000/a.rseg" 2026-11-16T19:39:00.000Z 900 \
  "t/$H/m/l0/0000/old.rseg" 2026-11-16T19:30:00.000Z 900 \
  "t/$H/l/admission/p.snapshot" 2026-11-16T19:40:10.000Z 80 \
  "t/$H/a/alert-lease" 2026-11-16T19:40:18.900Z 100 > "$TMP/l0"
cp "$TMP/l0" "$TMP/l1"
sed -i "s#^t/$H/l/admission/p.snapshot\t[^\t]*#t/$H/l/admission/p.snapshot\t2026-11-16T20:05:00.000Z#" "$TMP/l1"
printf '%s\t%s\t%s\n' \
  "t/$H/m/l0/0000/b.rseg" 2026-11-16T20:00:00.000Z 900 \
  "t/$H/m/l1/0000/t.rseg" 2026-11-16T20:10:00.000Z 900 >> "$TMP/l1"
grep -v -e '/l1/0000/t.rseg' -e '/old.rseg' "$TMP/l1" > "$TMP/l2"
printf '%s\t%s\t%s\n' "t/$H/m/l0/0000/early.rseg" 2026-11-16T19:40:00.000Z 900 >> "$TMP/l2"
cp "$TMP/l2" "$TMP/l3"
printf '%s\t%s\t%s\n' "t/$H/m/l0/0000/td.rseg" 2026-11-16T22:30:00.000Z 900 >> "$TMP/l3"
{
  drill_post_fence_events running "$FENCE" "$TMP/l0" "$TMP/l1" "$TMP/l2"
  drill_post_fence_events teardown "$FENCE" "$TMP/l2" "$TMP/l3"
} > "$TMP/ev"
for want in \
  "write	t/$H/m/l0/0000/b.rseg	2026-11-16T20:00:00.000Z	running" \
  "write	t/$H/m/l1/0000/t.rseg	2026-11-16T20:10:00.000Z	running" \
  "write	t/$H/l/admission/p.snapshot	2026-11-16T20:05:00.000Z	running" \
  "remove	t/$H/m/l1/0000/t.rseg	2026-11-16T20:10:00.000Z	running" \
  "remove	t/$H/m/l0/0000/old.rseg	2026-11-16T19:30:00.000Z	running" \
  "write	t/$H/m/l0/0000/td.rseg	2026-11-16T22:30:00.000Z	teardown"; do
  grep -qxF "$want" "$TMP/ev" && ok "event: $(echo "$want" | cut -f1,2,4 | tr '\t' ' ')" || bad "event missing: $want"
done
[ "$(grep -c . "$TMP/ev")" -eq 6 ] && ok "exactly six post-fence events" || bad "event count: $(cat "$TMP/ev")"
grep -qF early.rseg "$TMP/ev" && bad "a key last modified before the fence counted as post-fence" || ok "pre-fence LastModified is not a post-fence event"
drill_post_fence_events running "$FENCE" "$TMP/l0" "$TMP/l2" > "$TMP/ev-pair"
grep -qF /l1/0000/t.rseg "$TMP/ev-pair" && bad "pair listing saw the transient key" || ok "only the snapshot chain sees a key written and removed between listings"
drill_post_fence_events running "$FENCE" "$TMP/l0" "$TMP/l1" "$TMP/l1" "$TMP/l2" > "$TMP/ev-dup"
[ "$(grep -c . "$TMP/ev-dup")" -eq 5 ] && ok "a repeated listing adds no duplicate event" || bad "dup events: $(cat "$TMP/ev-dup")"
# A key seen written by two pairs (listed, gone, listed again) is one event.
drill_post_fence_events running "$FENCE" "$TMP/l0" "$TMP/l1" "$TMP/l0" "$TMP/l1" > "$TMP/ev-twice"
[ "$(grep -c . "$TMP/ev-twice")" -eq 5 ] && ok "an event two pairs show is printed once" || bad "twice events: $(cat "$TMP/ev-twice")"

# Classes, one row per enumerated case: op, key relative to t/<h>/ (or
# absolute when it starts with "/"), LastModified, phase, class.
R=2026-11-16T20:00:00.000Z
while read -r op rel lm phase want; do
  [ -n "$op" ] || continue
  case "$rel" in
    /*) key=${rel#/} ;;
    *) key=t/$H/$rel ;;
  esac
  got=$(drill_post_fence_class "$op" "$key" "$lm" "$phase" "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS")
  [ "$got" = "$want" ] && ok "class $op $rel $phase = $want" || bad "class $op $rel $lm $phase = $got, want $want"
done << EOF
write /sys/maintain/workers/x $R running root
write /t/$OTHER/m/l0/0000/x.rseg $R running root
write m/admission/p.snapshot 2026-11-16T22:30:00.000Z running exempt-advisory
remove s/admission/p.snapshot $R running exempt-advisory
write l/maint/scrub/0003.cursor 2026-11-16T22:30:00.000Z running exempt-advisory
remove m/maint/scrub/0000.cursor $R running maintenance
write m/admission/x/p.snapshot $R running maintenance
write m/meta $R running prerequisite-only
write l/meta $R running maintenance
write u/l0/0001/x.rseg 2026-11-16T23:30:00.000Z running prerequisite-only
write u/c/0001/20261116T23/x.cmt 2026-11-16T23:30:00.000Z teardown prerequisite-only
write u/maint/0001/cursor $R running prerequisite-only
remove a/l0/0000/x.rseg $R running prerequisite-only
write enc $R running prerequisite-only
write config $R running prerequisite-only
write a/alert-lease 2026-11-16T19:41:21.900Z running alerts-grace
write a/alert-lease 2026-11-16T19:41:22.000Z running alerts-late
write a/state/latest 2026-11-16T19:45:00.000Z running alerts-late
write a/l0/0000/x.rseg 2026-11-16T19:41:00.000Z running alerts-grace
write a/c/0000/20261116T19/x.cmt 2026-11-16T19:45:00.000Z teardown alerts-late
write a/other 2026-11-16T19:41:00.000Z running unclassified
write m/l0/0000/x.rseg 2026-11-16T22:01:00.999Z running ingest-grace
write m/l0/0000/x.rseg 2026-11-16T22:01:01.000Z running l0c-after-horizon
write s/c/0000/20261116T21/x.cmt 2026-11-16T22:01:01.000Z running l0c-after-horizon
write l/c/0001/20261116T22/x.cmt 2026-11-16T22:30:00.000Z teardown teardown-data
write m/l0/0000/x.rseg 2026-11-16T19:50:00.000Z teardown teardown-data
write l/idem/k.20261116T19.idm 2026-11-16T19:50:00.000Z running idem
write s/idem/k.20261116T19.idm 2026-11-16T23:50:00.000Z running idem
write m/idem/k.20261116T19.idm $R running maintenance
write catalog/m/HEAD $R running maintenance
write m/l1/0000/x.rseg $R running maintenance
remove m/l0/0000/x.rseg $R running maintenance
remove l/idem/k.20261116T19.idm $R running maintenance
write m/prov $R running maintenance
write m/maint/0001/cursor $R running maintenance
write foo $R running unclassified
write catalog/u/HEAD $R running unclassified
EOF

# Class figures: one event of every recorded class passes, each figure
# present once.
cat > "$TMP/ev-pass" << EOF
write	sys/maintain/workers/x	$R	running
write	t/$H/m/admission/p.snapshot	$R	running
write	t/$H/m/meta	$R	running
write	t/$H/a/alert-lease	2026-11-16T19:41:00.000Z	running
write	t/$H/m/l0/0000/x.rseg	$R	running
write	t/$H/m/l0/0000/y.rseg	$R	teardown
write	t/$H/l/idem/k.20261116T19.idm	$R	running
EOF
: > "$TMP/cls-report"
if drill_classify_events "$TMP/ev-pass" "$TMP/cls" "$TMP/cls-report" warm "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > "$TMP/cls-out"; then
  ok "one event of every recorded class passes"
else
  bad "recorded classes failed: $(cat "$TMP/cls-out")"
fi
[ "$(grep -c '^band-ok ' "$TMP/cls-out")" -eq 11 ] && ok "eleven class figures checked" || bad "class figures: $(cat "$TMP/cls-out")"
grep -qxF "figure class_teardown-data_warm 1" "$TMP/cls-report" && grep -qxF "figure class_unclassified_warm 0" "$TMP/cls-report" \
  && ok "class counts written as figures, zero included" || bad "class report: $(cat "$TMP/cls-report")"
grep -qxF "teardown-data	write	t/$H/m/l0/0000/y.rseg	$R	teardown" "$TMP/cls" \
  && ok "classified event keeps its key, LastModified and phase" || bad "classes file: $(cat "$TMP/cls")"

# Each fail-row class fails the check alone and is named.
for extra in \
  "alerts-late|write	t/$H/a/state/latest	2026-11-16T19:45:00.000Z	running" \
  "l0c-after-horizon|write	t/$H/m/c/0000/20261116T22/x.cmt	2026-11-16T22:30:00.000Z	running" \
  "maintenance|remove	t/$H/m/l0/0000/a.rseg	$R	running" \
  "unclassified|write	t/$H/foo	$R	running"; do
  cls=${extra%%|*}
  { cat "$TMP/ev-pass"; printf '%s\n' "${extra#*|}"; } > "$TMP/ev-bad"
  : > "$TMP/cls-report"
  if drill_classify_events "$TMP/ev-bad" "$TMP/cls" "$TMP/cls-report" warm "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > "$TMP/cls-out"; then
    bad "one $cls event passed"
  else
    ok "one $cls event fails"
  fi
  grep -qxF "band-miss class_${cls}_warm=1 outside [0, 0]" "$TMP/cls-out" && ok "$cls named in the miss" || bad "$cls miss: $(cat "$TMP/cls-out")"
done
# A report that already holds a class figure fails it: exactly once.
printf 'figure class_root_warm 0\n' > "$TMP/cls-report"
drill_classify_events "$TMP/ev-pass" "$TMP/cls" "$TMP/cls-report" warm "$H" "$HORIZON" "$ALERTS" "$EXEMPT_PATTERNS" > "$TMP/cls-out" \
  && bad "duplicated class figure passed" || ok "duplicated class figure fails"

# 2xx responses requested after a limit: the driver's curl -m 30 bound.
printf '%s\t%s\t%s\n' \
  $((TRUST - 5)) otlp-metrics 200 \
  $((TRUST + 30)) otlp-metrics 200 \
  $((TRUST + 31)) otlp-logs 503 \
  $((TRUST + 31)) promql 200 \
  $((TRUST + 40)) otlp-traces 000 > "$TMP/drive-pass"
[ "$(drill_count_2xx_after "$TMP/drive-pass" "$TRUST" 'otlp-metrics|otlp-logs|otlp-traces|remote-write' 30)" -eq 0 ] \
  && ok "no ingest 2xx requested after the trust limit counts 0" || bad "ingest pass case counted"
{ cat "$TMP/drive-pass"; printf '%s\t%s\t%s\n' $((TRUST + 31)) remote-write 204; } > "$TMP/drive-fail"
[ "$(drill_count_2xx_after "$TMP/drive-fail" "$TRUST" 'otlp-metrics|otlp-logs|otlp-traces|remote-write' 30)" -eq 1 ] \
  && ok "a 204 remote write requested after the trust limit counts 1" || bad "ingest fail case not counted"
[ "$(drill_count_2xx_after "$TMP/drive-pass" "$TRUST" promql 30)" -eq 1 ] && ok "a 200 query requested after the fence counts 1" || bad "query not counted"
[ "$(drill_count_2xx_after "$TMP/drive-pass" $((TRUST + 1)) promql 30)" -eq 0 ] && ok "a query that may predate the limit counts 0" || bad "query inside the curl bound counted"
[ "$(drill_count_2xx_after "$TMP/drive-fail" "$TRUST" 'remote' 30)" -eq 0 ] && ok "kind must match whole" || bad "kind matched a prefix"

# Data-bearing versions.
DM="t/$H/m/l0/0000/w.1.00000000000000000007.0011223344556677.rseg"
CM="t/$H/m/c/0000/20261116T22/w.1.00000000000000000007.cmt"
DL="t/$H/l/l0/0001/w.1.00000000000000000003.8899aabbccddeeff.rseg"
CL="t/$H/l/c/0001/20261116T22/w.1.00000000000000000003.cmt"
DS="t/$H/s/l0/0000/w.1.00000000000000000002.0102030405060708.rseg"
T=2026-11-16T23:02:39.000Z
printf '%s\t%s\t%s\n' "$DM" "$T" 900 "$CM" "$T" 300 "$DL" "$T" 900 "$CL" "$T" 300 "$DS" "$T" 900 > "$TMP/dv-listing"
printf '%s\t%s\t%s\n' "$DM" RSG1 7 "$DL" RLG1 5 "$DS" RSP1 5 > "$TMP/dv-trailers"
for k in "$DM" "$CM" "$DL" "$CL" "$DS"; do
  drill_data_version "$k" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-trailers" > "$TMP/dv" \
    && ok "release version passes: ${k#"t/$H/"}" || bad "release version failed: $(cat "$TMP/dv")"
done
drill_data_version "$CM" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-trailers" > "$TMP/dv" || true
grep -qxF "ok commit $CM $T data=$DM RSG1 7" "$TMP/dv" && ok "commit record checked through its data object" || bad "commit line: $(cat "$TMP/dv")"
for wrong in "$DM RSG1 8" "$DS RSP1 4" "$DL RSG1 5" "$DL RLG1 6"; do
  set -- $wrong
  awk -F'\t' -v k="$1" -v m="$2" -v v="$3" 'BEGIN { OFS = "\t" } $1 == k { $2 = m; $3 = v } { print }' \
    "$TMP/dv-trailers" > "$TMP/dv-wrong"
  drill_data_version "$1" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-wrong" > /dev/null && bad "data object at $2 $3 passed" || ok "data object at $2 $3 fails"
done
awk -F'\t' -v k="$DM" 'BEGIN { OFS = "\t" } $1 == k { $3 = 8 } { print }' "$TMP/dv-trailers" > "$TMP/dv-wrong"
drill_data_version "$CM" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-wrong" > "$TMP/dv" && bad "commit over an RSEG 8 object passed" || ok "commit record inherits its data object's version failure"
grep -v "^$DS" "$TMP/dv-trailers" > "$TMP/dv-missing"
drill_data_version "$DS" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-missing" > /dev/null && bad "unread trailer passed" || ok "unread trailer fails"
{ cat "$TMP/dv-trailers"; printf '%s\t%s\t%s\n' "$DS" RSP1 5; } > "$TMP/dv-twice"
drill_data_version "$DS" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-twice" > /dev/null && bad "trailer read twice passed" || ok "a key with two trailer lines fails"
grep -v "^$DL" "$TMP/dv-listing" > "$TMP/dv-nodata"
drill_data_version "$CL" "$T" "$H" "$TMP/dv-nodata" "$TMP/dv-trailers" > "$TMP/dv" && bad "unpaired commit passed" || ok "unpaired commit record fails"
grep -qxF "bad commit $CL $T no single paired data object" "$TMP/dv" && ok "unpaired commit named" || bad "unpaired line: $(cat "$TMP/dv")"
drill_data_version "t/$H/l/idem/k.20261116T22.idm" "$T" "$H" "$TMP/dv-listing" "$TMP/dv-trailers" > "$TMP/dv" \
  && bad "idem object passed with no trailer rule" || ok "idem object has no version rule and fails"
grep -qxF "bad other t/$H/l/idem/k.20261116T22.idm $T no version rule covers this key" "$TMP/dv" && ok "idem named" || bad "idem line: $(cat "$TMP/dv")"

# Census: every judged key named once and flagged.
printf '%s\n' "$DM" "$CM" "$DL" > "$TMP/cn-keys"
cat > "$TMP/cn-out" << EOF
certify family=rseg target=8 report=Contradicted
key $DM flagged=contradicted
key $CM flagged=late-write
key $DL flagged=held
census keys=3 flagged=3 not_flagged=0
EOF
drill_census_check "$TMP/cn-keys" "$TMP/cn-out" > "$TMP/cn" && ok "every key flagged passes" || bad "flagged census failed: $(cat "$TMP/cn")"
grep -qxF "flagged held $DL" "$TMP/cn" && ok "flag kind recorded" || bad "census lines: $(cat "$TMP/cn")"
sed "s#^key $CM flagged=late-write#key $CM flagged=no#" "$TMP/cn-out" > "$TMP/cn-no"
drill_census_check "$TMP/cn-keys" "$TMP/cn-no" > "$TMP/cn" && bad "unflagged key passed" || ok "one unflagged key fails"
grep -qxF "not-flagged $CM" "$TMP/cn" && ok "unflagged key named" || bad "census lines: $(cat "$TMP/cn")"
grep -v "^key $DL " "$TMP/cn-out" > "$TMP/cn-missing"
drill_census_check "$TMP/cn-keys" "$TMP/cn-missing" > "$TMP/cn" && bad "unjudged key passed" || ok "a key the census never printed fails"
{ cat "$TMP/cn-out"; echo "key $DM flagged=contradicted"; } > "$TMP/cn-dup"
drill_census_check "$TMP/cn-keys" "$TMP/cn-dup" > /dev/null && bad "key printed twice passed" || ok "a key printed twice fails"
printf 'key %s flagged=maybe\n' "$DM" > "$TMP/cn-odd"
printf '%s\n' "$DM" > "$TMP/cn-one"
drill_census_check "$TMP/cn-one" "$TMP/cn-odd" > /dev/null && bad "unknown flag passed" || ok "an unknown flag value fails"
drill_census_check /dev/null "$TMP/cn-out" > /dev/null && ok "no data-bearing keys passes" || bad "empty key list failed"

# Warm observation: tail, a scrub cursor after the horizon, graceful stops.
printf 'exempt-advisory\twrite\tt/%s/m/maint/scrub/0000.cursor\t2026-11-16T22:40:00.000Z\trunning\n' "$H" > "$TMP/wo-cls"
echo "graceful $((HORIZON + 3660))" > "$TMP/wo-server"
echo "graceful $((HORIZON + 3661))" > "$TMP/wo-maint"
: > "$TMP/wo-report"
drill_warm_observation "$TMP/wo-report" "$TMP/wo-cls" "$HORIZON" 3660 "$TMP/wo-server" "$TMP/wo-maint" > "$TMP/wo" \
  && ok "61 min tail, a cursor after the horizon and two graceful stops pass" || bad "warm observation failed: $(cat "$TMP/wo")"
echo "graceful $((HORIZON + 3659))" > "$TMP/wo-short"
: > "$TMP/wo-report"
drill_warm_observation "$TMP/wo-report" "$TMP/wo-cls" "$HORIZON" 3660 "$TMP/wo-short" "$TMP/wo-maint" > "$TMP/wo" \
  && bad "a 3659 s tail passed" || ok "a tail one second short fails"
grep -qxF "band-miss warm_tail_s=3659 outside [3660, 1000000000]" "$TMP/wo" && ok "short tail named" || bad "tail line: $(cat "$TMP/wo")"
for how in killed absent; do
  echo "$how $((HORIZON + 3700))" > "$TMP/wo-m2"
  : > "$TMP/wo-report"
  drill_warm_observation "$TMP/wo-report" "$TMP/wo-cls" "$HORIZON" 3660 "$TMP/wo-server" "$TMP/wo-m2" > /dev/null \
    && bad "a $how maintain process passed" || ok "a $how maintain process fails the graceful stop"
done
sed 's/2026-11-16T22:40:00.000Z/2026-11-16T22:01:00.000Z/' "$TMP/wo-cls" > "$TMP/wo-cls-early"
: > "$TMP/wo-report"
drill_warm_observation "$TMP/wo-report" "$TMP/wo-cls-early" "$HORIZON" 3660 "$TMP/wo-server" "$TMP/wo-maint" > /dev/null \
  && bad "a cursor only up to the horizon passed" || ok "no scrub cursor after the horizon fails"
sed 's#maint/scrub/0000.cursor#admission/p.snapshot#' "$TMP/wo-cls" > "$TMP/wo-cls-adm"
: > "$TMP/wo-report"
drill_warm_observation "$TMP/wo-report" "$TMP/wo-cls-adm" "$HORIZON" 3660 "$TMP/wo-server" "$TMP/wo-maint" > /dev/null \
  && bad "an admission snapshot counted as a scrub rotation" || ok "only a scrub cursor counts as a rotation"

# Trailer parsing: version is the u16 LE at bytes 8..10, magic the last 4.
printf 'payload\x05\x00\x00\x00\xaa\xbb\xcc\xdd\x07\x00\x01\x00RSG1' > "$TMP/obj"
tl=$(python3 -I "$HERE/drill_s3.py" trailer-file "$TMP/obj" | cut -f2-)
[ "$tl" = "RSG1	7	1" ] && ok "trailer-file reads RSG1 version 7 signal 1" || bad "trailer-file gave '$tl'"
printf '\x05\x00\x00\x00\xaa\xbb\xcc\xdd\x05\x01\x02\x00RLG1' > "$TMP/obj2"
tl=$(python3 -I "$HERE/drill_s3.py" trailer-file "$TMP/obj2" | cut -f2-)
[ "$tl" = "RLG1	261	2" ] && ok "trailer version is little-endian u16" || bad "trailer-file gave '$tl'"
printf 'short' > "$TMP/obj3"
python3 -I "$HERE/drill_s3.py" trailer-file "$TMP/obj3" > /dev/null 2>&1 && bad "short object read as a trailer" || ok "short object refused"

# Removed keys.
drill_removed_keys "$TMP/l1" "$TMP/l0" > "$TMP/removed"
[ "$(grep -c . "$TMP/removed")" -eq 2 ] && ok "removed keys found" || bad "removed count $(grep -c . "$TMP/removed")"

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
