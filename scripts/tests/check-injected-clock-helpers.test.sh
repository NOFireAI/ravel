#!/usr/bin/env bash
# Coverage for check-injected-clock-helpers.sh (issue #1260): the scan must
# find the injected-clock test helpers by structure (any fn item in the
# #[cfg(test)] module whose body or signature mentions an injected-clock type
# -- TestClock or FixedClock -- plus the two named helpers) rather than by a
# hand-maintained line list, flag the wall-clock constructs (including a bare/
# aliased sleep() call and .elapsed()) only inside that scanned region, honor
# the single-line allow-wall-clock marker only when it carries a non-empty
# reason and is not merely inside a string literal, and fail outright -- not
# pass silently -- when the scan finds no helpers at all.
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

# The 1-indexed line number of the first line matching a fixed pattern.
line_of() {
  grep -nF -- "$2" "$1" | head -1 | cut -d: -f1
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

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]
