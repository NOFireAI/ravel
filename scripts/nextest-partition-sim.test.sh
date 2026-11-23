#!/usr/bin/env bash
# Cases for nextest-partition-sim.py. Add a case here before changing a
# behaviour.
#
# The fixture is twelve real tests with the leg nextest's `hash:M/4` put each
# on in CI run 45003110822 (check-shard), three per leg, written the way a
# downloaded CI job log carries them: a timestamp, ANSI colour codes, and
# nextest's `PASS [ Ns] (k/n) binary test` line. If nextest changed its rule,
# replaying a fresh run's logs would fail the way the swapped-legs case does.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="$HERE/nextest-partition-sim.py"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fails=0
check() { # check <name> <expected-exit> <expected-substring> <args...>
  local name="$1" want_rc="$2" want_sub="$3"; shift 3
  local out rc=0
  out="$(python3 -I "$SCRIPT" "$@" 2>&1)" || rc=$?
  if [ "$rc" != "$want_rc" ]; then
    echo "FAIL $name: exit $rc, wanted $want_rc"; echo "$out" | sed 's/^/    /'; fails=$((fails+1)); return
  fi
  case "$out" in
    *"$want_sub"*) echo "ok   $name" ;;
    *) echo "FAIL $name: output missing '$want_sub'"; echo "$out" | sed 's/^/    /'; fails=$((fails+1)) ;;
  esac
}

E=$'\x1b'
line() { # line <seconds> <binary> <test>
  printf '2026-11-22T08:40:00.0000000Z %s[32m        PASS%s[0m [%8ss] (  1/4102) %s[35m%s%s[0m %s[36m%s%s[0m\n' \
    "$E" "$E" "$1" "$E" "$2" "$E" "$E" "$3" "$E"
}
{
  line 0.005 ravel-affinity tests::ordering_independence_shuffled_input_same_result
  line 0.005 ravel-affinity tests::bounded_reassignment_on_addition
  line 100.000 ravel-esql::errors unexpected_character
} > "$TMP/leg1.log"
{
  line 0.006 ravel-affinity tests::empty_replicas_and_size_zero_do_not_panic
  line 0.006 ravel-affinity tests::size_at_or_above_len_returns_all_ranked_no_truncation
  line 20.000 ravel-analytics::fixtures step_fixture
  # A SLOW progress line carries no (k/n) count and must not be read as a result.
  printf '2026-11-22T08:41:00.0000000Z         SLOW [> 60.000s] ravel-analytics::fixtures step_fixture\n'
} > "$TMP/leg2.log"
{
  line 0.006 ravel-affinity tests::determinism_same_inputs_same_output
  line 0.007 ravel-affinity tests::bounded_reassignment_on_removal
  line 30.000 ravel-analytics::fixtures dip_fixture
} > "$TMP/leg3.log"
{
  line 0.006 ravel-affinity tests::size_one_returns_single_hrw_winner
  line 0.007 ravel-affinity tests::size_two_returns_top_two_in_hrw_order
  line 3.061 ravel-bench::s3_e2e_smoke memory_smoke
} > "$TMP/leg4.log"
: > "$TMP/empty.log"

# The published XXH64 test vectors, seed 0: the empty input and "abc".
check "xxh64 of the empty input" 0 "ef46db3751d8e999" hash ""
check "xxh64 of abc"             0 "44bc2cf5ad770999" hash abc

check "replay of the recorded split" 0 "replayed 12 tests over 4 legs: 0 misassigned" \
  replay "$TMP/leg1.log" "$TMP/leg2.log" "$TMP/leg3.log" "$TMP/leg4.log"
check "replay with two legs swapped" 1 "ran on leg 1, the rule gives leg 2" \
  replay "$TMP/leg2.log" "$TMP/leg1.log" "$TMP/leg3.log" "$TMP/leg4.log"
check "replay of a log with no results" 2 "no nextest result lines" \
  replay "$TMP/empty.log"

# At 4 legs the sums are the files' own: leg 1 holds the 100 s test.
check "simulate at the recorded count" 0 "legs=4 slowest=100s" \
  simulate --legs 4 "$TMP/leg1.log" "$TMP/leg2.log" "$TMP/leg3.log" "$TMP/leg4.log"
check "simulate counts every test once" 0 "12 tests, 153 summed seconds" \
  simulate --legs 4,5 "$TMP/leg1.log" "$TMP/leg2.log" "$TMP/leg3.log" "$TMP/leg4.log"
check "a band the slowest leg sits inside" 0 "band legs=4 [90, 110]: slowest 100s inside" \
  simulate --legs 4 --band 4:90:110 "$TMP/leg1.log" "$TMP/leg2.log" "$TMP/leg3.log" "$TMP/leg4.log"
check "a band the slowest leg misses" 1 "band legs=4 [10, 50]: slowest 100s OUTSIDE" \
  simulate --legs 4 --band 4:10:50 "$TMP/leg1.log" "$TMP/leg2.log" "$TMP/leg3.log" "$TMP/leg4.log"
check "a band for a count not simulated" 64 "names a leg count --legs does not" \
  simulate --legs 4 --band 5:10:50 "$TMP/leg1.log"
check "a malformed leg list" 64 "--legs wants a list" \
  simulate --legs four "$TMP/leg1.log"

if [ "$fails" -ne 0 ]; then
  echo "nextest-partition-sim.test.sh: $fails case(s) failed"
  exit 1
fi
echo "nextest-partition-sim.test.sh: all cases passed"
