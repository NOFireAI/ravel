#!/usr/bin/env bash
# Coverage for check-injected-clock-helpers.sh (issue #1260): the scan must
# find the injected-clock test helpers by structure (any fn item whose body
# or signature mentions TestClock, plus the two named helpers) rather than by
# a hand-maintained line list, flag the five wall-clock constructs only
# inside that scanned region, honor the single-line allow-wall-clock marker,
# and fail outright -- not pass silently -- when the scan finds no helpers at
# all (the TestClock-renamed regression this check exists to catch).
#
# Pure shell against throwaway fixture files under $TMPDIR: never the real
# services/ravel-cli/src/load.rs, so this suite cannot drift with the source
# the way asserting against the real file would.
#
# Run: bash scripts/tests/check-injected-clock-helpers.test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CHECK="${SCRIPT_DIR}/check-injected-clock-helpers.sh"

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

check_true() {
  local label="$1" cond="$2"
  if [[ "${cond}" == "1" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n' "${label}"
  fi
}

# A fixture shaped like services/ravel-cli/src/load.rs's test module: a
# TestClock type, the two helpers #1260 names by name, a third TestClock-
# mentioning helper (yield_until_router_is_quiet, to prove the scan is not
# just the two named functions), and one ordinary test with no clock at all.
# That is 4 scanned helpers on a clean fixture (TestClock::new,
# yield_until_router_is_quiet, load_with_released_tail,
# load_two_writes_across_one_clock_advance).
write_base_fixture() {
  local file="$1"
  cat >"${file}" <<'RS'
pub struct Thing;

#[cfg(test)]
mod tests {
    use super::*;

    struct TestClock {
        now_ns: i64,
    }

    impl TestClock {
        fn new(start_ns: i64) -> Self {
            TestClock { now_ns: start_ns }
        }
    }

    async fn yield_until_router_is_quiet(clock: &TestClock) {
        let _ = clock;
        tokio::task::yield_now().await;
    }

    async fn load_with_released_tail(clock: &TestClock) -> usize {
        let _ = clock;
        0
    }

    async fn load_two_writes_across_one_clock_advance(clock: &TestClock) -> usize {
        let _ = clock;
        0
    }

    #[test]
    fn some_other_test() {
        assert_eq!(1 + 1, 2);
    }
}
RS
}

# === (a) a clean fixture passes with the exact helper count ================
clean="${tmproot}/clean.rs"
write_base_fixture "${clean}"

out="$(bash "${CHECK}" "${clean}" 2>&1)"
rc=$?
check_eq "clean fixture exits 0" "0" "${rc}"
check_eq "clean fixture reports the exact scanned-helper count" \
  "check-injected-clock-helpers.sh: clean (4 helper(s) scanned)" "${out}"

# === (b) a tokio::time::sleep inside a TestClock helper fails with exit 1,
#     naming the file, line and symbol =====================================
sleeping="${tmproot}/sleeping.rs"
write_base_fixture "${sleeping}"
# Line 23 is `        let _ = clock;` inside load_with_released_tail; insert
# the offending call right after it.
sed -i '23a\        tokio::time::sleep(std::time::Duration::from_millis(1)).await;' "${sleeping}"

out="$(bash "${CHECK}" "${sleeping}" 2>&1)"
rc=$?
check_eq "a sleep in a TestClock helper exits 1" "1" "${rc}"
check_true "the finding names file:line:symbol in the helper" \
  "$(printf '%s\n' "${out}" | grep -qF "${sleeping}:24: tokio::time::sleep in load_with_released_tail" && echo 1 || echo 0)"

# === (c) the same line with a trailing allow-wall-clock marker passes ======
allowed="${tmproot}/allowed.rs"
write_base_fixture "${allowed}"
sed -i '23a\        tokio::time::sleep(std::time::Duration::from_millis(1)).await; // allow-wall-clock: fixture reason' "${allowed}"

out="$(bash "${CHECK}" "${allowed}" 2>&1)"
rc=$?
check_eq "an allow-wall-clock marker on the offending line suppresses the finding" "0" "${rc}"
check_eq "the allowlisted fixture still reports 4 scanned helpers" \
  "check-injected-clock-helpers.sh: clean (4 helper(s) scanned)" "${out}"

# === (d) a wall-clock call OUTSIDE any injected-clock helper passes: the
#     check is scoped to TestClock helpers, not a file-wide grep ============
outside="${tmproot}/outside.rs"
write_base_fixture "${outside}"
# some_other_test never mentions TestClock, so a real sleep in it must not
# be flagged.
sed -i 's/        assert_eq!(1 + 1, 2);/        std::thread::sleep(std::time::Duration::from_millis(1));\n        assert_eq!(1 + 1, 2);/' "${outside}"

out="$(bash "${CHECK}" "${outside}" 2>&1)"
rc=$?
check_eq "a wall-clock call outside any TestClock helper exits 0" "0" "${rc}"
check_eq "the out-of-scope fixture still reports 4 scanned helpers" \
  "check-injected-clock-helpers.sh: clean (4 helper(s) scanned)" "${out}"

# === (e) a fixture whose helpers no longer mention TestClock fails with the
#     zero-helpers error, not a silent pass =================================
renamed="${tmproot}/renamed.rs"
write_base_fixture "${renamed}"
sed -i \
  -e 's/TestClock/FakeClock/g' \
  -e 's/load_with_released_tail/totally_renamed_helper/g' \
  -e 's/load_two_writes_across_one_clock_advance/totally_renamed_helper_2/g' \
  "${renamed}"

out="$(bash "${CHECK}" "${renamed}" 2>&1)"
rc=$?
check_eq "a fixture with no TestClock mention and renamed named-helpers exits 1" "1" "${rc}"
check_true "the zero-helpers error names the file and the two expected helpers" \
  "$(printf '%s\n' "${out}" | grep -qF "0 injected-clock helpers scanned in ${renamed}" && echo 1 || echo 0)"

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]
