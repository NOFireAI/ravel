# Decision logic of the released-binary fence drill
# (scripts/compat/released-binary-drill.sh), sourced by the drill and by
# scripts/compat/released-binary-drill.test.sh. No network, no state: every
# function reads files and prints or returns.
#
# A listing file holds one key per line: "<key>\t<LastModified>\t<size>",
# LastModified as ISO 8601 UTC (what ListObjectsV2 returns).

# Keys written between two listings: present in $2 and absent from $1, or
# present in both with a different LastModified or size (an overwrite).
# Prints "<key>\t<LastModified>\t<size>" lines from $2.
drill_written_keys() {
  local before=$1 after=$2
  awk -F'\t' 'NR == FNR { seen[$1] = $2 "\t" $3; next }
              !($1 in seen) || seen[$1] != ($2 "\t" $3) { print }' "$before" "$after"
}

# Keys present in $1 and absent from $2 (deleted between the listings).
drill_removed_keys() {
  local before=$1 after=$2
  awk -F'\t' 'NR == FNR { now[$1] = 1; next } !($1 in now) { print }' "$after" "$before"
}

# ISO 8601 UTC to Unix seconds (fraction dropped).
drill_epoch() {
  date -u -d "$1" +%s
}

# The v0.33.0 ingest grace horizon for a fence written at Unix second $1:
# a v0.33.0 router trusts its cached provisioning view until the hour
# hour_of(R) + 2, R being its last successful refresh (R <= the fence), and a
# flush opened in the last trusted hour publishes until the end of that hour
# plus max_flush_lifetime (3600 s, compiled in). So the latest publish is
# (hour_of(fence) + 3) * 3600, plus the store request timeout margin $2.
drill_ingest_horizon() {
  local fence=$1 margin=$2
  echo $(( (fence / 3600 + 3) * 3600 + margin ))
}

# The last second a running v0.33.0 router may still accept ingest when R,
# Unix second $1, is its last successful provisioning refresh before the
# fence: it trusts that cached view until the hour hour_of(R) + 2
# (min_lead_hours of the 60 s refresh interval, v0.33.0 ravel-ingest
# generation.rs). A process started after the fence has no view to trust;
# its limit is the fence itself, which the caller passes instead.
drill_trust_limit() {
  echo $(( ($1 / 3600 + 2) * 3600 ))
}

# A lower bound on R for the routers serving kinds $3 (a regex over the drive
# log's kind column), from drive log $1 and the second the fence began, $2.
# A router refreshes its view on the first write after the view is older
# than the refresh interval $4, so the view that served the last 2xx write
# ending at or before $2 was refreshed at most $4 seconds before that request
# began, and the request began at most $5 seconds (the driver's curl -m)
# before it ended. Prints that end second minus $4 minus $5; returns 1 when
# no such write exists. v0.33.0 does not log its refreshes, so a later R is
# possible and the limit drill_trust_limit gives for this bound may be an hour
# early, never late.
drill_last_refresh_floor() {
  awk -F'\t' -v t="$2" -v k="^($3)\$" -v r="$4" -v m="$5" \
    '$1 <= t && $2 ~ k && $3 ~ /^2[0-9][0-9]$/ && $1 > last { last = $1 }
     END { if (last == "") exit 1; print last - r - m }' "$1"
}

# The drive log's kinds per signal router of v0.33.0: remote write lands in
# the metrics router.
DRILL_INGEST_ROUTERS="metrics:otlp-metrics|remote-write logs:otlp-logs spans:otlp-traces"

# Trust table for a running process: one "<signal>\t<kinds>\t<R floor>\t<limit>"
# line per router in DRILL_INGEST_ROUTERS, from drive log $1, the second the
# fence began $2, refresh interval $3 and request bound $4. A router with no
# 2xx write before the fence gets "none" for both and the table returns 1.
drill_trust_table() {
  local log=$1 start=$2 refresh=$3 margin=$4 r sig kinds floor rc=0
  for r in $DRILL_INGEST_ROUTERS; do
    sig=${r%%:*}
    kinds=${r#*:}
    if floor=$(drill_last_refresh_floor "$log" "$start" "$kinds" "$refresh" "$margin"); then
      printf '%s\t%s\t%s\t%s\n' "$sig" "$kinds" "$floor" "$(drill_trust_limit "$floor")"
    else
      printf '%s\t%s\tnone\tnone\n' "$sig" "$kinds"
      rc=1
    fi
  done
  return "$rc"
}

# Trust table for a process started after the fence, Unix second $1: every
# router's limit is the fence, and its floor column is "cold".
drill_trust_table_cold() {
  local r
  for r in $DRILL_INGEST_ROUTERS; do
    printf '%s\t%s\tcold\t%s\n' "${r%%:*}" "${r#*:}" "$1"
  done
}

# Ingest 2xx responses in drive log $1 requested after their router's limit
# in trust table $2, request bound $3 (drill_count_2xx_after). Prints the
# total; returns 1 unless the table holds exactly one line per router of
# DRILL_INGEST_ROUTERS, each with an integer limit.
drill_ingest_2xx_after_trust() {
  local log=$1 table=$2 margin=$3 r sig kinds n limit total=0
  for r in $DRILL_INGEST_ROUTERS; do
    sig=${r%%:*}
    kinds=${r#*:}
    n=$(awk -F'\t' -v s="$sig" -v k="$kinds" '$1 == s && $2 == k { n++ } END { print n + 0 }' "$table")
    limit=$(awk -F'\t' -v s="$sig" -v k="$kinds" '$1 == s && $2 == k { print $4 }' "$table")
    case "$n:$limit" in
      1:'' | 1:*[!0-9]*) n=0 ;;
    esac
    if [ "$n" -ne 1 ]; then
      echo "missing"
      return 1
    fi
    total=$((total + $(drill_count_2xx_after "$log" "$limit" "$kinds" "$margin")))
  done
  echo "$total"
}

# Count of keys in listing $1 whose key starts with $2.
drill_count_prefix() {
  awk -F'\t' -v p="$2" 'index($1, p) == 1 { n++ } END { print n + 0 }' "$1"
}

# Figure check, per CLAUDE.md "Measurement and cost discipline": report $1
# must hold exactly one "figure <name> <value>" line for $2, with an integer
# value inside [$3, $4]. Prints "band-ok"/"band-miss" lines; returns 1 on a
# miss.
drill_check_figure() {
  local report=$1 name=$2 lo=$3 hi=$4 n v
  n=$(awk -v k="$name" '$1 == "figure" && $2 == k { n++ } END { print n + 0 }' "$report")
  if [ "$n" -ne 1 ]; then
    echo "band-miss $name present $n times (want exactly 1)"
    return 1
  fi
  v=$(awk -v k="$name" '$1 == "figure" && $2 == k { print $3 }' "$report")
  case "$v" in
    '' | *[!0-9]*)
      echo "band-miss $name value '$v' is not a non-negative integer"
      return 1
      ;;
  esac
  if [ "$v" -lt "$lo" ] || [ "$v" -gt "$hi" ]; then
    echo "band-miss $name=$v outside [$lo, $hi]"
    return 1
  fi
  echo "band-ok $name=$v in [$lo, $hi]"
}

# Whether key $1 of tenant hash $2 matches one of the exempt patterns $3
# (whitespace-separated globs relative to t/<h>/). A glob matches only a key
# with the same number of path segments, so "*" never spans a "/".
drill_exempt() {
  local key=$1 h=$2 patterns=$3 rel p
  case "$key" in
    "t/$h/"*) rel=${key#"t/$h/"} ;;
    *) return 1 ;;
  esac
  for p in $patterns; do
    # shellcheck disable=SC2053
    if [[ $rel == $p ]] && [ "${rel//[!\/]/}" = "${p//[!\/]/}" ]; then
      return 0
    fi
  done
  return 1
}

# Trailer the release writes for a data object under signal prefix $1:
# "<magic> <version>". RSEG 7 for metrics, RLOG 5 for logs and alerts,
# RSPAN 5 for spans (v0.33.0 ravel-ingest config.rs).
drill_release_trailer() {
  case "$1" in
    m) echo "RSG1 7" ;;
    l | a) echo "RLG1 5" ;;
    s) echo "RSP1 5" ;;
    *) return 1 ;;
  esac
}

# Data-object key a commit record key $1 pairs with in listing $2: same
# tenant, signal, shard and <writer>.<epoch>.<seq>. Prints the one match;
# returns 1 when there is none or more than one.
drill_commit_data_key() {
  local key=$1 listing=$2 base sig shard stem
  base=${key%/*}
  stem=${key##*/}
  stem=${stem%.cmt}
  # base is t/<h>/<sig>/c/<shard>/<hour>
  sig=$(echo "$base" | cut -d/ -f3)
  shard=$(echo "$base" | cut -d/ -f5)
  awk -F'\t' -v p="$(echo "$base" | cut -d/ -f1-2)/$sig/l0/$shard/$stem." \
    'index($1, p) == 1 && $1 ~ /\.rseg$/ && substr($1, length(p) + 1) !~ /\// { print $1; n++ }
     END { exit n == 1 ? 0 : 1 }' "$listing"
}

# Positive control (a): every write path the drill drives landed keys, per
# prefix under t/<h>/. Appends "figure <name> <count>" lines to report $3
# and checks each is present once and at least 1; returns 1 otherwise.
PC_PREFIXES="m/l0 m/c l/l0 l/c s/l0 s/c catalog/m catalog/l catalog/s m/admission l/admission s/admission a/alert-lease a/state a/l0 a/c u/l0 u/c"
positive_control() {
  local listing=$1 h=$2 report=$3 p name fail=0
  for p in $PC_PREFIXES; do
    name="pc_$(echo "$p" | tr '/' '_')"
    echo "figure $name $(drill_count_prefix "$listing" "t/$h/$p")" >> "$report"
  done
  for p in $PC_PREFIXES; do
    name="pc_$(echo "$p" | tr '/' '_')"
    drill_check_figure "$report" "$name" 1 1000000 || fail=1
  done
  return "$fail"
}

pc_ready() {
  local listing=$1 h=$2 p
  for p in $PC_PREFIXES; do
    [ "$(drill_count_prefix "$listing" "t/$h/$p")" -ge 1 ] || return 1
  done
}

# Post-fence events over a chain of listings taken in time order: $1 is the
# phase name (running or teardown), $2 the fence's Unix second, $3.. the
# listings. Prints "<op>\t<key>\t<LastModified>\t<phase>" once per distinct
# event: op "write" for a key some pair shows written with a LastModified
# after the fence, "remove" (LastModified of the last listing that held it)
# for a key some pair shows removed. A key written and removed between two
# listings is invisible here, which is why the drill lists the warm bucket
# more than once while it runs.
drill_post_fence_events() {
  local phase=$1 fence=$2 prev="" cur key lm size
  shift 2
  for cur in "$@"; do
    if [ -n "$prev" ]; then
      drill_written_keys "$prev" "$cur" | while IFS=$'\t' read -r key lm size; do
        if [ "$(drill_epoch "$lm")" -gt "$fence" ]; then
          printf 'write\t%s\t%s\t%s\n' "$key" "$lm" "$phase"
        fi
      done
      drill_removed_keys "$prev" "$cur" | while IFS=$'\t' read -r key lm size; do
        printf 'remove\t%s\t%s\t%s\n' "$key" "$lm" "$phase"
      done
    fi
    prev=$cur
  done | awk '!seen[$0]++'
}

# Class of one post-fence event (ADR-4233 decision 4 enumeration on issue
# #4243, revised pre-registration): op $1, key $2, LastModified $3, phase $4,
# tenant hash $5, ingest horizon $6, alerts horizon $7 (Unix seconds), exempt
# patterns $8 (drill_exempt globs).
#   root                  not under t/<h>/: recorded, outside the fenced scope
#   exempt-advisory       a write matching $8, or a remove of an admission
#                         snapshot (the sibling DELETE)
#   audit-unfenced        u/l0/... and u/c/... written: per-query audit
#                         records, the "audit: unfenced by design" class
#                         (decision 4 does not fence audit); recorded
#   prerequisite-only     m/meta, any other u/... event (u/l1, u/maint,
#                         audit removals), a/... removals (alert retention
#                         and sweep), enc, config
#   alerts-grace          a/alert-lease, a/state/, a/l0/, a/c/ written up to
#                         the alerts horizon
#   alerts-late           the same written after it                    (fails)
#   ingest-grace          <sig>/l0/...rseg, <sig>/c/...cmt written while
#                         running, up to the ingest horizon
#   l0c-after-horizon     the same written while running, after it     (fails)
#   teardown-data         the same written at the graceful stop  (data-bearing)
#   idem                  l/idem/<k>.<hour>.idm, s/idem/...      (data-bearing)
#   maintenance           any other write or remove under <sig>/ or
#                         catalog/<sig>/: fold, compaction, retention,
#                         sweep, rewrite, maint cursor, .unn          (fails)
#   unclassified          any other t/<h>/ event                       (fails)
# <sig> is m, l or s.
drill_post_fence_class() {
  local op=$1 key=$2 lm=$3 phase=$4 h=$5 ingest_h=$6 alerts_h=$7 exempt=$8 rel t
  case "$key" in
    "t/$h/"*) rel=${key#"t/$h/"} ;;
    *)
      echo root
      return
      ;;
  esac
  t=$(drill_epoch "$lm")
  if { [ "$op" = write ] && drill_exempt "$key" "$h" "$exempt"; } \
    || { [ "$op" = remove ] && [[ $rel =~ ^[mls]/admission/[^/]+\.snapshot$ ]]; }; then
    echo exempt-advisory
  elif [ "$op" = write ] && [[ $rel =~ ^u/(l0|c)/ ]]; then
    echo audit-unfenced
  elif [ "$rel" = m/meta ] || [ "$rel" = enc ] || [ "$rel" = config ] || [[ $rel == u/* ]] \
    || { [ "$op" = remove ] && [[ $rel == a/* ]]; }; then
    echo prerequisite-only
  elif [ "$op" = write ] && { [ "$rel" = a/alert-lease ] || [[ $rel =~ ^a/(state|l0|c)/ ]]; }; then
    if [ "$t" -gt "$alerts_h" ]; then echo alerts-late; else echo alerts-grace; fi
  elif [ "$op" = write ] && [[ $rel =~ ^[mls]/(l0/.*\.rseg|c/.*\.cmt)$ ]]; then
    if [ "$phase" = teardown ]; then
      echo teardown-data
    elif [ "$t" -gt "$ingest_h" ]; then
      echo l0c-after-horizon
    else
      echo ingest-grace
    fi
  elif [ "$op" = write ] && [[ $rel =~ ^[ls]/idem/[^/]+\.idm$ ]]; then
    echo idem
  elif [[ $rel =~ ^(catalog/)?[mls]/ ]]; then
    echo maintenance
  else
    echo unclassified
  fi
}

# Every class drill_post_fence_class prints, and the band its count must sit
# in: the fail rows at [0, 0], the recorded classes at [0, 1000000].
DRILL_CLASS_BANDS="root:0:1000000 exempt-advisory:0:1000000 audit-unfenced:0:1000000 prerequisite-only:0:1000000 alerts-grace:0:1000000 alerts-late:0:0 ingest-grace:0:1000000 l0c-after-horizon:0:0 teardown-data:0:1000000 idem:0:1000000 maintenance:0:0 unclassified:0:0"

# Classifies events file $1 (drill_post_fence_events output) into $2 as
# "<class>\t<op>\t<key>\t<LastModified>\t<phase>", appends one
# "figure class_<class>_<tag> <n>" line per class in DRILL_CLASS_BANDS to
# report $3 ($4 is the tag) and checks each. $5..$8: tenant hash, ingest
# horizon, alerts horizon, exempt patterns. Returns 1 when any count misses
# its band.
drill_classify_events() {
  local events=$1 out=$2 report=$3 tag=$4 h=$5 ingest_h=$6 alerts_h=$7 exempt=$8 op key lm phase c b name lo hi rc=0
  : > "$out"
  while IFS=$'\t' read -r op key lm phase; do
    [ -n "$key" ] || continue
    c=$(drill_post_fence_class "$op" "$key" "$lm" "$phase" "$h" "$ingest_h" "$alerts_h" "$exempt")
    printf '%s\t%s\t%s\t%s\t%s\n' "$c" "$op" "$key" "$lm" "$phase" >> "$out"
  done < "$events"
  for b in $DRILL_CLASS_BANDS; do
    IFS=: read -r name lo hi <<< "$b"
    echo "figure class_${name}_$tag $(awk -F'\t' -v c="$name" '$1 == c { n++ } END { print n + 0 }' "$out")" >> "$report"
  done
  for b in $DRILL_CLASS_BANDS; do
    IFS=: read -r name lo hi <<< "$b"
    drill_check_figure "$report" "class_${name}_$tag" "$lo" "$hi" || rc=1
  done
  return "$rc"
}

# Responses of kinds $3 (a regex over the drive log's kind column) with a 2xx
# status whose end second is more than $4 seconds after Unix second $2, in
# drive log $1 ("<end second>\t<kind>\t<status>"). The driver bounds every
# request at $4 seconds (curl -m), so such a response was requested after $2.
drill_count_2xx_after() {
  awk -F'\t' -v t="$2" -v k="^($3)\$" -v m="$4" \
    '$1 > t + m && $2 ~ k && $3 ~ /^2[0-9][0-9]$/ { n++ } END { print n + 0 }' "$1"
}

# Version check of one data-bearing key $1 (LastModified $2) for tenant hash
# $3 against listing $4 and trailer file $5 ("<key>\t<magic>\t<version>"): a
# data object must carry the release's trailer for its signal, a commit
# record's single paired data object must, and an idem object has no trailer
# rule so it fails. Prints one "ok|bad <what> <key> <LastModified> ..." line
# and returns 1 on bad.
drill_data_version() {
  local key=$1 lm=$2 h=$3 listing=$4 trailers=$5 rel sig data want got
  rel=${key#"t/$h/"}
  sig=${rel%%/*}
  case "$rel" in
    [mls]/l0/*.rseg) data=$key ;;
    [mls]/c/*.cmt)
      data=$(drill_commit_data_key "$key" "$listing") || {
        echo "bad commit $key $lm no single paired data object"
        return 1
      }
      ;;
    *)
      echo "bad other $key $lm no version rule covers this key"
      return 1
      ;;
  esac
  want=$(drill_release_trailer "$sig")
  got=$(awk -F'\t' -v k="$data" '$1 == k { print $2 " " $3; n++ } END { exit n == 1 ? 0 : 1 }' "$trailers") || {
    echo "bad data $key $lm trailer of $data not read"
    return 1
  }
  if [ "$got" != "$want" ]; then
    echo "bad version $key $lm data=$data trailer '$got' want '$want'"
    return 1
  fi
  if [ "$data" = "$key" ]; then
    echo "ok data $key $lm $got"
  else
    echo "ok commit $key $lm data=$data $got"
  fi
}

# Warm observation row: one full scrub rotation after the ingest horizon $3
# and a graceful stop. Appends to report $1, from classes file $2
# (drill_classify_events output) and the stop records $5.. (stop_pid's
# "<how> <Unix second>" files, the server's first):
#   warm_tail_s                       server stop second minus $3, >= $4
#   warm_scrub_cursor_after_horizon   exempt scrub cursor writes after $3, >= 1
#   warm_graceful_stops               "graceful" stop records, all of them
# Returns 1 when any misses its band.
drill_warm_observation() {
  local report=$1 classes=$2 horizon=$3 tail=$4 stopped n lm rc=0
  shift 4
  stopped=$(cut -d' ' -f2 "$1")
  n=0
  while IFS= read -r lm; do
    if [ "$(drill_epoch "$lm")" -gt "$horizon" ]; then n=$((n + 1)); fi
  done < <(awk -F'\t' '$1 == "exempt-advisory" && $2 == "write" && $3 ~ /\/maint\/scrub\/[^\/]+\.cursor$/ { print $4 }' "$classes")
  {
    echo "figure warm_tail_s $((stopped - horizon))"
    echo "figure warm_scrub_cursor_after_horizon $n"
    echo "figure warm_graceful_stops $(cat "$@" | grep -c '^graceful ' || true)"
  } >> "$report"
  drill_check_figure "$report" warm_tail_s "$tail" 1000000000 || rc=1
  drill_check_figure "$report" warm_scrub_cursor_after_horizon 1 1000000 || rc=1
  drill_check_figure "$report" warm_graceful_stops "$#" "$#" || rc=1
  return "$rc"
}
