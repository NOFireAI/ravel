#!/usr/bin/env bash
# Coverage for check-injected-clock-helpers.sh (issue #1260): the scan must
# find the injected-clock test helpers by structure (any fn item in the
# #[cfg(test)] module whose body or signature mentions an injected-clock type
# -- TestClock or FixedClock -- plus the two named helpers) rather than by a
# hand-maintained line list, flag the wall-clock constructs only inside that
# scanned region, honor the single-line allow-wall-clock marker only when it
# carries a non-empty reason and is not merely inside a string literal, and
# fail outright -- not pass silently -- when the scan finds no helpers at all
# or fewer of them than the real default target holds.
#
# Every one of the gate's seven wall-clock patterns has at least one case
# here, so deleting any single pattern line from the gate fails this suite:
# thread::sleep, tokio::time::sleep, tokio::time::timeout, Instant::,
# SystemTime, .elapsed() and a bare/aliased sleep() call. The two
# receiver-form patterns also have to exempt an injected clock's own
# sleep/elapsed while still flagging any other receiver.
#
# Portability: fixtures are written whole with heredocs, never mutated with
# `sed -i` (whose backup-extension argument differs between GNU and BSD sed),
# so this suite runs unchanged on Linux and macOS.
#
# Stub-resistance: every case asserts the gate's exact exit code AND either
# the exact finding text (file:line: symbol in helper, with the line derived
# from the fixture so a wrong line fails) or the exact clean-line helper
# count. A gate that only echoes a clean line and exits 0 fails nearly every
# case. Prove it: `CHECK_OVERRIDE=/path/to/stub bash <this-file>`.
#
# Run: bash scripts/tests/check-injected-clock-helpers.test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CHECK="${CHECK_OVERRIDE:-${SCRIPT_DIR}/check-injected-clock-helpers.sh}"

tmproot="$(mktemp -d "${TMPDIR:-/tmp}/check-injected-clock-helpers-test.XXXXXX")"
if [[ ! -d "${tmproot}" ]]; then
  echo "FAIL  could not create a temp dir for the fixtures" >&2
  exit 1
fi
# Physical path, so the paths this test compares against equal the ones the
# script's own printf %s reports (a symlinked $TMPDIR would otherwise mismatch).
tmproot="$(cd "${tmproot}" && pwd -P)"
trap 'rm -rf "${tmproot}"' EXIT

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

# Assert that ${haystack} contains the exact fixed line ${needle}.
check_contains() {
  local label="$1" haystack="$2" needle="$3"
  if printf '%s\n' "${haystack}" | grep -qF -- "${needle}"; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  expected to contain: %s\n  in:                  %s\n' \
      "${label}" "${needle}" "${haystack}"
  fi
}

# Assert that ${haystack} does NOT contain the fixed substring ${needle}.
check_absent() {
  local label="$1" haystack="$2" needle="$3"
  if printf '%s\n' "${haystack}" | grep -qF -- "${needle}"; then
    fail=$((fail + 1))
    printf 'FAIL  %s\n  did not expect: %s\n  in:             %s\n' \
      "${label}" "${needle}" "${haystack}"
  else
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  fi
}

# Assert that ${got} is an integer at or above ${floor}.
check_ge() {
  local label="$1" floor="$2" got="$3"
  if [[ "${got}" =~ ^[0-9]+$ ]] && ((got >= floor)); then
    pass=$((pass + 1))
    printf 'ok    %s (%s >= %s)\n' "${label}" "${got}" "${floor}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: an integer >= %s\n  got:  %s\n' "${label}" "${floor}" "${got}"
  fi
}

# The 1-indexed line number of the first line matching a fixed pattern.
line_of() {
  grep -nF -- "$2" "$1" | head -1 | cut -d: -f1
}

# The scanned-helper count out of a clean run's report line, or "" if the run
# did not print one. Parsed from an already-captured string, so no pipeline
# ever masks the gate's own exit code.
helper_count_of() {
  printf '%s\n' "$1" | sed -n 's/^.*clean (\([0-9]\{1,\}\) helper(s) scanned)$/\1/p'
}

# === (a) a clean fixture passes with the exact helper count ================
# Three scanned helpers: one keyed on TestClock, one on FixedClock, one via
# the named-helper fallback with no clock mention. `some_other_test` mentions
# no clock and is not named, so it is not scanned.
clean="${tmproot}/clean.rs"
cat >"${clean}" <<'RS'
pub struct Thing;

#[cfg(test)]
mod tests {
    use super::*;

    struct TestClock {
        now_ns: i64,
    }
    struct FixedClock(i64);

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        tokio::task::yield_now().await;
    }

    async fn helper_on_fixedclock() {
        let _c = FixedClock(0);
    }

    async fn load_with_released_tail() -> usize {
        0
    }

    #[test]
    fn some_other_test() {
        assert_eq!(1 + 1, 2);
    }
}
RS

out="$(bash "${CHECK}" "${clean}" 2>&1)"
rc=$?
check_eq "clean fixture exits 0" "0" "${rc}"
check_eq "clean fixture reports the exact scanned-helper count" \
  "check-injected-clock-helpers.sh: clean (3 helper(s) scanned)" "${out}"

# === (b) a tokio::time::sleep inside a TestClock helper fails, naming the
#     file, line and symbol ==================================================
sleeping="${tmproot}/sleeping.rs"
cat >"${sleeping}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}
RS
ln="$(line_of "${sleeping}" 'tokio::time::sleep(std::time::Duration')"
out="$(bash "${CHECK}" "${sleeping}" 2>&1)"
rc=$?
check_eq "a sleep in a TestClock helper exits 1" "1" "${rc}"
check_contains "the finding names file:line: symbol in helper" "${out}" \
  "${sleeping}:${ln}: tokio::time::sleep in helper_on_testclock"

# === (c) a FixedClock helper's sleep is flagged too (proves the predicate
#     covers FixedClock, not just TestClock) =================================
fixedsleep="${tmproot}/fixedsleep.rs"
cat >"${fixedsleep}" <<'RS'
#[cfg(test)]
mod tests {
    struct FixedClock(i64);

    async fn waits_under_a_fixed_clock() {
        let _c = FixedClock(0);
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
}
RS
ln="$(line_of "${fixedsleep}" 'tokio::time::sleep(std::time::Duration')"
out="$(bash "${CHECK}" "${fixedsleep}" 2>&1)"
rc=$?
check_eq "a sleep in a FixedClock helper exits 1" "1" "${rc}"
check_contains "the FixedClock finding names file:line: symbol in helper" "${out}" \
  "${fixedsleep}:${ln}: tokio::time::sleep in waits_under_a_fixed_clock"

# === (d) the same line with a trailing allow-wall-clock marker and a reason
#     passes =================================================================
allowed="${tmproot}/allowed.rs"
cat >"${allowed}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await; // allow-wall-clock: fixture reason
    }
}
RS
out="$(bash "${CHECK}" "${allowed}" 2>&1)"
rc=$?
check_eq "an allow-wall-clock marker with a reason suppresses the finding" "0" "${rc}"
check_eq "the allowlisted fixture still reports 1 scanned helper" \
  "check-injected-clock-helpers.sh: clean (1 helper(s) scanned)" "${out}"

# === (e) an allow-wall-clock marker with an EMPTY reason does NOT suppress ==
emptyreason="${tmproot}/emptyreason.rs"
cat >"${emptyreason}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await; // allow-wall-clock:
    }
}
RS
ln="$(line_of "${emptyreason}" 'tokio::time::sleep(std::time::Duration')"
out="$(bash "${CHECK}" "${emptyreason}" 2>&1)"
rc=$?
check_eq "an empty-reason marker still exits 1" "1" "${rc}"
check_contains "the empty-reason finding is still reported" "${out}" \
  "${emptyreason}:${ln}: tokio::time::sleep in helper_on_testclock"

# === (f) a string literal that merely contains the marker text does NOT
#     suppress a real finding on the same line ===============================
stringmarker="${tmproot}/stringmarker.rs"
cat >"${stringmarker}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        let _n = "// allow-wall-clock: not a real marker"; tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}
RS
ln="$(line_of "${stringmarker}" 'not a real marker')"
out="$(bash "${CHECK}" "${stringmarker}" 2>&1)"
rc=$?
check_eq "a marker inside a string literal does not suppress" "1" "${rc}"
check_contains "the string-literal case still reports the finding" "${out}" \
  "${stringmarker}:${ln}: tokio::time::sleep in helper_on_testclock"

# === (g) an aliased `use ...::sleep;` followed by a bare sleep() call is
#     caught as sleep() =======================================================
aliased="${tmproot}/aliased.rs"
cat >"${aliased}" <<'RS'
#[cfg(test)]
mod tests {
    use std::thread::sleep;
    struct TestClock;

    fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        sleep(std::time::Duration::from_millis(1));
    }
}
RS
ln="$(line_of "${aliased}" 'sleep(std::time::Duration::from_millis(1));')"
out="$(bash "${CHECK}" "${aliased}" 2>&1)"
rc=$?
check_eq "an aliased bare sleep() call exits 1" "1" "${rc}"
check_contains "the aliased call is reported as sleep()" "${out}" \
  "${aliased}:${ln}: sleep() in helper_on_testclock"

# === (h) a `.elapsed()` with no Instant:: on the line is caught ============
elapsed="${tmproot}/elapsed.rs"
cat >"${elapsed}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    fn helper_on_testclock(clock: &TestClock, start: std::time::Instant) {
        let _ = clock;
        let _d = start.elapsed();
    }
}
RS
ln="$(line_of "${elapsed}" 'let _d = start.elapsed();')"
out="$(bash "${CHECK}" "${elapsed}" 2>&1)"
rc=$?
check_eq "a bare .elapsed() exits 1" "1" "${rc}"
check_contains "the .elapsed() call is reported" "${out}" \
  "${elapsed}:${ln}: .elapsed() in helper_on_testclock"

# === (i) a wall-clock call OUTSIDE any injected-clock helper passes: the
#     check is scoped to clock helpers, not a file-wide grep =================
outside="${tmproot}/outside.rs"
cat >"${outside}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
    }

    #[test]
    fn some_other_test() {
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert_eq!(1 + 1, 2);
    }
}
RS
out="$(bash "${CHECK}" "${outside}" 2>&1)"
rc=$?
check_eq "a wall-clock call outside any clock helper exits 0" "0" "${rc}"
check_eq "the out-of-scope fixture reports only the 1 clock helper" \
  "check-injected-clock-helpers.sh: clean (1 helper(s) scanned)" "${out}"
check_absent "no finding is emitted for the out-of-scope sleep" "${out}" \
  "some_other_test"

# === (j) a fixture whose helpers no longer mention any clock type fails with
#     the zero-helpers error, not a silent pass ==============================
renamed="${tmproot}/renamed.rs"
cat >"${renamed}" <<'RS'
#[cfg(test)]
mod tests {
    struct WallClock;

    fn totally_renamed_helper(clock: &WallClock) {
        let _ = clock;
    }
}
RS
out="$(bash "${CHECK}" "${renamed}" 2>&1)"
rc=$?
check_eq "a fixture with no clock mention and no named helper exits 1" "1" "${rc}"
check_contains "the zero-helpers error names the file" "${out}" \
  "0 injected-clock helpers scanned in ${renamed}"

# === (k) self-check: a fixture with a known helper set reports the exact
#     count and names, so a gate that silently scans nothing (or a stub with
#     a hardcoded count) fails ===============================================
knownset="${tmproot}/knownset.rs"
cat >"${knownset}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;
    struct FixedClock(i64);

    fn a_testclock_helper(c: &TestClock) { let _ = c; }
    fn a_fixedclock_helper() { let _c = FixedClock(0); }
    fn load_two_writes_across_one_clock_advance() {}
    fn load_with_released_tail() {}
    fn a_plain_test() { assert!(true); }
}
RS
out="$(bash "${CHECK}" "${knownset}" 2>&1)"
rc=$?
# Four scanned: two clock-keyed, two named-fallback. a_plain_test is neither.
check_eq "the known-set fixture exits 0" "0" "${rc}"
check_eq "the known-set fixture reports exactly 4 scanned helpers" \
  "check-injected-clock-helpers.sh: clean (4 helper(s) scanned)" "${out}"

# === (l) thread::sleep is flagged =========================================
# One case per remaining wall-clock pattern, so deleting any single pattern
# line from the gate fails at least one case here. Before these, only
# tokio::time::sleep and the two receiver-form patterns were covered, and four
# of the five symbols issue #1260 names by name could be deleted outright with
# the suite still green.
threadsleep="${tmproot}/threadsleep.rs"
cat >"${threadsleep}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
RS
ln="$(line_of "${threadsleep}" 'std::thread::sleep(std::time::Duration')"
out="$(bash "${CHECK}" "${threadsleep}" 2>&1)"
rc=$?
check_eq "a thread::sleep in a clock helper exits 1" "1" "${rc}"
check_contains "the thread::sleep finding names file:line: symbol in helper" "${out}" \
  "${threadsleep}:${ln}: thread::sleep in helper_on_testclock"

# === (m) tokio::time::timeout is flagged ==================================
timeout_fx="${tmproot}/timeout.rs"
cat >"${timeout_fx}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), work()).await;
    }
}
RS
ln="$(line_of "${timeout_fx}" 'tokio::time::timeout(std::time::Duration')"
out="$(bash "${CHECK}" "${timeout_fx}" 2>&1)"
rc=$?
check_eq "a tokio::time::timeout in a clock helper exits 1" "1" "${rc}"
check_contains "the timeout finding names file:line: symbol in helper" "${out}" \
  "${timeout_fx}:${ln}: tokio::time::timeout in helper_on_testclock"

# === (n) Instant:: is flagged =============================================
instant="${tmproot}/instant.rs"
cat >"${instant}" <<'RS'
#[cfg(test)]
mod tests {
    struct FixedClock(i64);

    fn helper_on_fixedclock() {
        let _c = FixedClock(0);
        let _started = std::time::Instant::now();
    }
}
RS
ln="$(line_of "${instant}" 'std::time::Instant::now();')"
out="$(bash "${CHECK}" "${instant}" 2>&1)"
rc=$?
check_eq "an Instant:: in a clock helper exits 1" "1" "${rc}"
check_contains "the Instant:: finding names file:line: symbol in helper" "${out}" \
  "${instant}:${ln}: Instant:: in helper_on_fixedclock"

# === (o) SystemTime is flagged ============================================
systemtime="${tmproot}/systemtime.rs"
cat >"${systemtime}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        let _wall = std::time::SystemTime::now();
    }
}
RS
ln="$(line_of "${systemtime}" 'std::time::SystemTime::now();')"
out="$(bash "${CHECK}" "${systemtime}" 2>&1)"
rc=$?
check_eq "a SystemTime in a clock helper exits 1" "1" "${rc}"
check_contains "the SystemTime finding names file:line: symbol in helper" "${out}" \
  "${systemtime}:${ln}: SystemTime in helper_on_testclock"

# === (p) an injected clock's OWN sleep/elapsed is not a wall-clock wait ====
# The idiom the guard exists to encourage: a helper that awaits the injected
# clock, in three receiver shapes. Reporting these would leave writing an
# allow-wall-clock reason for code that is not a wall-clock wait as the only
# escape.
clockrecv="${tmproot}/clockrecv.rs"
cat >"${clockrecv}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    async fn helper_awaiting_the_injected_clock(clock: &TestClock, holder: &Holder) {
        clock.sleep(std::time::Duration::from_millis(1)).await;
        let _d = clock.elapsed();
        holder.test_clock.as_ref().sleep(std::time::Duration::from_secs(1)).await;
    }
}
RS
out="$(bash "${CHECK}" "${clockrecv}" 2>&1)"
rc=$?
check_eq "awaiting the injected clock's own sleep/elapsed exits 0" "0" "${rc}"
check_eq "the clock-receiver fixture still reports 1 scanned helper" \
  "check-injected-clock-helpers.sh: clean (1 helper(s) scanned)" "${out}"

# === (q) the receiver exemption is scoped to clocks, not blanket ===========
otherrecv="${tmproot}/otherrecv.rs"
cat >"${otherrecv}" <<'RS'
#[cfg(test)]
mod tests {
    struct TestClock;

    fn helper_on_testclock(clock: &TestClock, timer: &Timer) {
        let _ = clock;
        timer.sleep(std::time::Duration::from_millis(1));
    }
}
RS
ln="$(line_of "${otherrecv}" 'timer.sleep(std::time::Duration')"
out="$(bash "${CHECK}" "${otherrecv}" 2>&1)"
rc=$?
check_eq "a non-clock receiver's sleep is still flagged" "1" "${rc}"
check_contains "the non-clock receiver finding is reported as sleep()" "${out}" \
  "${otherrecv}:${ln}: sleep() in helper_on_testclock"

# === (r) the real default target stays above the helper-count floor ========
# The zero-helpers guard only catches a predicate that finds NOTHING. This
# catches one that narrows: dropping FixedClock from the gate's CLOCK_TYPES
# takes the real file from 26 scanned helpers to 5, and dropping both clock
# types to 2, each of which passed every other case in this suite. A floor,
# not an exact count, so a new helper in load.rs never fails the gate.
REAL_TARGET_HELPER_FLOOR=20
check_eq "the gate declares the expected default-target helper floor" \
  "DEFAULT_TARGET_MIN_HELPERS=${REAL_TARGET_HELPER_FLOOR}" \
  "$(grep -m1 '^DEFAULT_TARGET_MIN_HELPERS=' "${CHECK}")"
out="$(bash "${CHECK}" 2>&1)"
rc=$?
check_eq "the real default target scans clean" "0" "${rc}"
check_ge "the real default target's scanned-helper count clears the floor" \
  "${REAL_TARGET_HELPER_FLOOR}" "$(helper_count_of "${out}")"

# === (s) the scan start is anchored on the stripped line ===================
# `scan_start` is found by matching the cfg(test) attribute. Matched on the RAW
# line, a doc comment or string literal naming that attribute starts the scan
# above the real test module, and production functions that mention a clock
# type then enter the scan. The wall-clock call below sits in production code
# and must NOT be reported.
anchor="${tmproot}/anchor.rs"
cat >"${anchor}" <<'RS'
//! Module docs that mention #[cfg(test)] in prose.

pub struct TestClock;

/// Production helper. Not a test, not scanned.
pub fn prod_helper_on_testclock(_c: &TestClock) {
    std::thread::sleep(std::time::Duration::from_millis(1));
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn helper_on_testclock(clock: &TestClock) {
        let _ = clock;
        tokio::task::yield_now().await;
    }

    #[test]
    fn t() {}
}
RS
out="$(bash "${CHECK}" "${anchor}" 2>&1)"
rc=$?
check_eq "a cfg(test) mention in a doc comment does not move the scan start" "0" "${rc}"
check_eq "only the real test module's helper is scanned past a doc-comment mention"   "1" "$(helper_count_of "${out}")"

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]
