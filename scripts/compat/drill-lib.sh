# Decision logic of the released-binary fence drill
# (scripts/compat/released-binary-drill.sh), sourced by the drill and by
# scripts/compat/released-binary-drill.test.sh. No network, no state: every
# function reads files and prints or returns.
#
# A listing file holds one key per line: "<key>\t<LastModified>\t<size>",
# LastModified as ISO 8601 UTC (what ListObjectsV2 returns).

# Class of a key for tenant hash $2:
#   fenced  t/<h>/{m,l,s}/... and t/<h>/catalog/{m,l,s}/...: a provisioned
#           (tenant, signal) whose record the drill raises to version 4
#   alerts  t/<h>/a/...: fenced by the parked alert lease
#   audit   t/<h>/u/...: not fenced (ADR-4233 decision 4, "audit: not fenced")
#   tenant  any other t/<h>/... key
#   other   everything else (sys/, other tenants)
drill_classify() {
  local key=$1 h=$2
  case "$key" in
    "t/$h/m/"* | "t/$h/l/"* | "t/$h/s/"*) echo fenced ;;
    "t/$h/catalog/m/"* | "t/$h/catalog/l/"* | "t/$h/catalog/s/"*) echo fenced ;;
    "t/$h/a/"*) echo alerts ;;
    "t/$h/u/"*) echo audit ;;
    "t/$h/"*) echo tenant ;;
    *) echo other ;;
  esac
}

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

# Late writes: from the written-keys file $1 (drill_written_keys output),
# every key of class fenced or alerts for tenant hash $2 whose LastModified is
# strictly after Unix second $3 (fenced) or $4 (alerts), unless it matches an
# exempt pattern of $5. Prints "late <class> <key> <LastModified>" per late
# write and "exempt <class> <key> <LastModified> after|within" per exempt
# write of either class (recorded, never a failure); returns 1 when any late
# write exists.
drill_late_writes() {
  local written=$1 h=$2 fenced_after=$3 alerts_after=$4 exempt=${5:-} key lm size class t limit found=0
  while IFS=$'\t' read -r key lm size; do
    [ -n "$key" ] || continue
    class=$(drill_classify "$key" "$h")
    case "$class" in
      fenced) limit=$fenced_after ;;
      alerts) limit=$alerts_after ;;
      *) continue ;;
    esac
    t=$(drill_epoch "$lm")
    if drill_exempt "$key" "$h" "$exempt"; then
      if [ "$t" -gt "$limit" ]; then
        echo "exempt $class $key $lm after"
      else
        echo "exempt $class $key $lm within"
      fi
    elif [ "$t" -gt "$limit" ]; then
      echo "late $class $key $lm"
      found=1
    fi
  done < "$written"
  return "$found"
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

# Teardown writes: the keys of written-keys file $1 (drill_written_keys
# between the last running listing and the one after the processes stopped)
# for tenant hash $2. $3 is the post-stop listing, $4 a trailer file of
# "<key>\t<magic>\t<version>" lines, $5 the exempt patterns. Every fenced or
# alerts key must be one of:
#   exempt          an exempt pattern: recorded
#   data object     <sig>/l0/.../*.rseg whose trailer is the release's
#   commit record   <sig>/c/.../*.cmt whose paired data object (in $3) has
#                   the release's trailer
# Anything else fails, since no version rule covers it. Prints one
# "teardown-ok|teardown-exempt|teardown-bad <what> <key> <LastModified> ..."
# line per key; returns 1 when any line is teardown-bad.
drill_teardown_check() {
  local written=$1 h=$2 listing=$3 trailers=$4 exempt=${5:-} key lm size class rel sig want got data found=0
  while IFS=$'\t' read -r key lm size; do
    [ -n "$key" ] || continue
    class=$(drill_classify "$key" "$h")
    case "$class" in
      fenced | alerts) ;;
      *) continue ;;
    esac
    if drill_exempt "$key" "$h" "$exempt"; then
      echo "teardown-exempt $class $key $lm"
      continue
    fi
    rel=${key#"t/$h/"}
    case "$rel" in
      catalog/*) sig="" ;;
      *) sig=${rel%%/*} ;;
    esac
    case "$rel" in
      */l0/*.rseg)
        data=$key
        ;;
      */c/*.cmt)
        if ! data=$(drill_commit_data_key "$key" "$listing"); then
          echo "teardown-bad commit $key $lm no single paired data object"
          found=1
          continue
        fi
        ;;
      *)
        echo "teardown-bad other $key $lm no version rule covers this key"
        found=1
        continue
        ;;
    esac
    want=$(drill_release_trailer "$sig") || {
      echo "teardown-bad other $key $lm no release trailer for signal '$sig'"
      found=1
      continue
    }
    got=$(awk -F'\t' -v k="$data" '$1 == k { print $2 " " $3; n++ } END { exit n == 1 ? 0 : 1 }' "$trailers") || {
      echo "teardown-bad data $key $lm trailer of $data not read"
      found=1
      continue
    }
    if [ "$got" = "$want" ]; then
      if [ "$data" = "$key" ]; then
        echo "teardown-ok data $key $lm $got"
      else
        echo "teardown-ok commit $key $lm data=$data $got"
      fi
    else
      echo "teardown-bad version $key $lm data=$data trailer '$got' want '$want'"
      found=1
    fi
  done < "$written"
  return "$found"
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
