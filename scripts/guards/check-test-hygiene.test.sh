#!/usr/bin/env bash
# Cases for check-test-hygiene.sh. Add a case here before changing a rule, the
# way .claude/guards/pretooluse.test.sh works for the hook.
#
# Each case builds a throwaway repo under $TMPDIR with the guard copied into
# its scripts/guards/, so the guard's own `cd repo_root` lands on the fixture
# and nothing here touches the real checkout.
#
# Run: bash scripts/guards/check-test-hygiene.test.sh
set -uo pipefail

# A developer's ambient git config can change what this suite measures: the
# guard runs `git check-ignore --no-index` before `git ls-files`, so a global
# core.excludesFile rule can make the tracked-seed case report a finding that
# has nothing to do with the fixture. Pin the fixtures to no system or global
# config so the cases assert the guard, not the host.
export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-test-hygiene.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-test-hygiene-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: a scratch repo with the guard installed. Prints its path.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards"
  cp "${GUARD}" "${dir}/scripts/guards/check-test-hygiene.sh"
  git -C "${dir}" init -q 2>/dev/null
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name Test
  printf '%s\n' "${dir}"
}

# check <name> <repo> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-test-hygiene.sh crates 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- wall-clock ------------------------------------------------------------

d="$(new_repo direct)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let start = Instant::now();
    work();
    assert!(start.elapsed() < Duration::from_secs(1), "too slow");
}
RS
check "flags an assert reading .elapsed() directly" "${d}" 1 "tests/t.rs:5: wall-clock"

d="$(new_repo derived)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let t0 = Instant::now();
    let a = t0.elapsed();
    let t1 = Instant::now();
    let b = t1.elapsed();
    let ratio = a.as_secs_f64() / b.as_secs_f64();
    assert!(ratio > 2.0, "the fast path must be faster");
}
RS
check "flags an assert on a value derived from a measurement" "${d}" 1 "reads \`ratio\`"

d="$(new_repo allowed)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let start = Instant::now();
    // hygiene-allow: wall-clock -- an #[ignore]d probe whose subject is wall time
    assert!(start.elapsed() < Duration::from_secs(1));
}
RS
check "an allow marker on the line above suppresses the finding" "${d}" 0 "clean"

d="$(new_repo allowed-block)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let start = Instant::now();
    let took = start.elapsed();
    // hygiene-allow: wall-clock -- the reason runs to several lines, because
    // the case genuinely cannot take an injected clock: the timing belongs to
    // a third-party client that accepts no clock from us, and the ceiling is
    // three orders of magnitude away from both outcomes.
    assert!(took < Duration::from_secs(1));
}
RS
check "an allow marker earlier in the same comment block suppresses it" "${d}" 0 "clean"

d="$(new_repo allowed-detached)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    // hygiene-allow: wall-clock -- detached from the assertion by real code
    let start = Instant::now();
    let took = start.elapsed();
    assert!(took < Duration::from_secs(1));
}
RS
check "a marker separated from the assertion by code does not suppress" "${d}" 1 "wall-clock"

# The regression that made the first draft of this guard noisy: production code
# times something into a report field, and a test asserts the field's presence.
# That is not a timing band and must not be flagged.
d="$(new_repo prod-field)"
mkdir -p "${d}/crates/c/src"
cat >"${d}/crates/c/src/lane.rs" <<'RS'
pub fn run() -> Report {
    let started = Instant::now();
    let out = work();
    let load_wall_ms = started.elapsed().as_secs_f64() * 1000.0;
    Report { out, load_wall_ms: Some(load_wall_ms) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_load_reports_none() {
        let report = Report::default();
        assert_eq!(report.load_wall_ms, None, "no load ran in this invocation");
    }
}
RS
check "a presence check on a production timing field is not a finding" "${d}" 0 "clean"

# A duration that is data, not elapsed real time.
d="$(new_repo semantic)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let span = decode();
    assert_eq!(span.duration_ns, 50, "duration_ns is end - start");
    assert!(elapsed_ns == 2 * SLOW.as_nanos() as u64);
}
RS
check "a semantic duration field is not a wall-clock band" "${d}" 0 "clean"

# issue #896: a tainted identifier named only inside an assertion message string
# must not be flagged. The message spans lines, so the taint word sits on a
# continuation line. Before the stripper carried string state across lines, this
# was reported as `assertion reads \`wall\``, and both ways out -- rewording the
# message or waiving a clock the assertion never reads -- made the tree worse.
d="$(new_repo msg-only-taint)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let t0 = Instant::now();
    let wall = t0.elapsed();
    record(wall);
    assert_eq!(
        objects_written(),
        BATCHES,
        "object layout is identical across arms: {BATCHES} objects however \
         the windows are set, so the wall comparison is not confounded by \
         a different number of PUTs"
    );
}
RS
check "a tainted name only inside a multi-line message is not flagged" "${d}" 0 "clean"

# The other side of the same change: a genuine timing assertion whose message
# happens to discuss timing is still flagged. Detection must not have weakened.
d="$(new_repo real-taint-with-msg)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let start = Instant::now();
    let wall = start.elapsed();
    assert!(
        wall < Duration::from_secs(1),
        "the wall clock must stay under a second"
    );
}
RS
check "a real timing assertion with a timing message is still flagged" "${d}" 1 "reads \`wall\`"

# A raw string carrying a // marker and an embedded quote must not desync the
# scan. Old per-line stripping paired the raw string's inner quotes as an
# ordinary string and left the tainted word between them exposed as code, a
# false positive; the raw-string form must strip the whole literal.
d="$(new_repo raw-string-comment)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let t0 = Instant::now();
    let wall = t0.elapsed();
    record(wall);
    assert_eq!(got, want, r#"a "wall" // divider between arms"#);
}
RS
check "a raw string with a comment marker does not desync the scan" "${d}" 0 "clean"

# Rust block comments hold code often enough that commented-out timing
# assertions are a real shape. Before this was handled the scan saw the code
# inside `/* ... */` and reported a violation from a line that never runs.
d="$(new_repo block-comment-code)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let start = Instant::now();
    /* an approach we dropped:
       assert!(start.elapsed() < Duration::from_secs(1), "too slow");
    */
    assert_eq!(rows_written, 60, "exact row count");
}
RS
check "code inside a block comment is not flagged" "${d}" 0 "clean"

# Rust block comments NEST, so a scan that closes on the first `*/` would
# resume parsing inside a comment and could flag the line after it.
d="$(new_repo block-comment-nested)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let start = Instant::now();
    /* outer /* inner */ still commented:
       assert!(start.elapsed() < Duration::from_secs(1), "no");
    */
    assert_eq!(objects, 8, "exact object count");
}
RS
check "a nested block comment closes at the right place" "${d}" 0 "clean"

# The inverse of the two above: a `/*` inside a string literal must not open a
# comment and swallow the genuine assertion that follows it.
d="$(new_repo slash-star-in-string)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let s = "/* not a comment";
    let start = Instant::now();
    assert!(start.elapsed() < Duration::from_secs(1), "fast");
}
RS
check "a /* inside a string does not open a comment" "${d}" 1 "wall-clock"

# A // comment holding an unbalanced quote must not open a string that swallows
# the rest of the file. If it did, the genuine timing assertion two lines down
# would vanish from the scan (a false negative). Order: // before any quote.
d="$(new_repo comment-unbalanced-quote)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    // this comment has an unbalanced " quote and a stray ) paren
    let start = Instant::now();
    let wall = start.elapsed();
    assert!(wall < Duration::from_secs(1), "fast");
}
RS
check "a comment with an unbalanced quote does not swallow the file" "${d}" 1 "wall-clock"

# --- unseeded randomness ---------------------------------------------------

d="$(new_repo entropy)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let mut rng = rand::rng();
    assert!(rng.random::<u64>() > 0);
}
RS
check "flags an entropy draw in a test file" "${d}" 1 "unseeded-rng"

d="$(new_repo seeded)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/t.rs" <<'RS'
#[test]
fn t() {
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    assert!(rng.random::<u64>() > 0);
}
RS
check "a seeded rng is clean" "${d}" 0 "clean"

# The sanctioned production seam: entropy above the test module in a src file.
d="$(new_repo prod-seam)"
mkdir -p "${d}/crates/c/src"
cat >"${d}/crates/c/src/rng.rs" <<'RS'
impl RngSource for SystemRng {
    fn jitter_ms(&self, max_ms: u64) -> u64 {
        rand::rng().random_range(0..=max_ms)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn t() {
        assert_eq!(SystemRng.jitter_ms(0), 0);
    }
}
RS
check "production entropy above the test module is not a finding" "${d}" 0 "clean"

# --- proptest seeds --------------------------------------------------------

d="$(new_repo seed-untracked)"
mkdir -p "${d}/crates/c/src" "${d}/crates/c/proptest-regressions"
cat >"${d}/crates/c/src/codec.rs" <<'RS'
proptest! {
    #[test]
    fn roundtrip(v in any::<u64>()) {
        prop_assert_eq!(decode(encode(v)), v);
    }
}
RS
printf 'cc 0102030405060708 # shrinks to v = 0\n' \
  >"${d}/crates/c/proptest-regressions/codec.txt"
check "flags a src/ regression seed that is not tracked" "${d}" 1 "proptest-seed"

d="$(new_repo seed-tracked)"
mkdir -p "${d}/crates/c/src" "${d}/crates/c/proptest-regressions"
cat >"${d}/crates/c/src/codec.rs" <<'RS'
proptest! {
    #[test]
    fn roundtrip(v in any::<u64>()) {
        prop_assert_eq!(decode(encode(v)), v);
    }
}
RS
printf 'cc 0102030405060708 # shrinks to v = 0\n' \
  >"${d}/crates/c/proptest-regressions/codec.txt"
git -C "${d}" add -A >/dev/null 2>&1
git -C "${d}" commit -qm seed >/dev/null 2>&1
check "a tracked src/ regression seed is clean" "${d}" 0 "clean"

d="$(new_repo seed-tests-dir)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/differential.rs" <<'RS'
proptest! {
    #[test]
    fn same(v in any::<u64>()) {
        prop_assert_eq!(a(v), b(v));
    }
}
RS
printf 'cc 0102030405060708 # shrinks to v = 0\n' \
  >"${d}/crates/c/tests/differential.proptest-regressions"
check "flags an untracked tests/<name>.proptest-regressions" "${d}" 1 \
  "tests/differential.proptest-regressions"

d="$(new_repo seed-ignored)"
mkdir -p "${d}/crates/c/tests"
cat >"${d}/crates/c/tests/differential.rs" <<'RS'
proptest! {
    #[test]
    fn same(v in any::<u64>()) {
        prop_assert_eq!(a(v), b(v));
    }
}
RS
printf 'cc 0102030405060708\n' \
  >"${d}/crates/c/tests/differential.proptest-regressions"
printf '*.proptest-regressions\n' >"${d}/.gitignore"
git -C "${d}" add -Af crates .gitignore >/dev/null 2>&1
git -C "${d}" commit -qm seed >/dev/null 2>&1
check "flags a tracked-but-gitignored seed" "${d}" 1 "gitignore"

d="$(new_repo no-seed-yet)"
mkdir -p "${d}/crates/c/src"
cat >"${d}/crates/c/src/codec.rs" <<'RS'
proptest! {
    #[test]
    fn roundtrip(v in any::<u64>()) {
        prop_assert_eq!(decode(encode(v)), v);
    }
}
RS
check "a proptest that has never caught anything is clean" "${d}" 0 "clean"

d="$(new_repo persistence-off)"
mkdir -p "${d}/crates/c/src"
cat >"${d}/crates/c/src/codec.rs" <<'RS'
proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..Default::default() })]
    #[test]
    fn roundtrip(v in any::<u64>()) {
        prop_assert_eq!(decode(encode(v)), v);
    }
}
RS
check "flags a failure_persistence override" "${d}" 1 "failure_persistence"

# --- usage -----------------------------------------------------------------

d="$(new_repo usage)"
mkdir -p "${d}/crates/c/src"
: >"${d}/crates/c/src/lib.rs"
out="$(cd "${d}" && bash scripts/guards/check-test-hygiene.sh --nope 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"unknown option"* ]]; then
  printf 'ok    an unknown option exits 64\n'
  passes=$((passes + 1))
else
  printf 'FAIL  an unknown option exits 64: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

# The two remaining documented usage paths. Both are behaviours the Makefile
# and CI depend on, and neither was asserted.
out="$(cd "${d}" && bash scripts/guards/check-test-hygiene.sh --help 2>&1)"
rc=$?
if [[ "${rc}" == "0" && "${out}" == *"test-hygiene"* ]]; then
  printf 'ok    --help prints the header and exits 0\n'
  passes=$((passes + 1))
else
  printf 'FAIL  --help prints the header and exits 0: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

empty="$(new_repo empty_root)"
mkdir -p "${empty}/crates"
out="$(cd "${empty}" && bash scripts/guards/check-test-hygiene.sh crates 2>&1)"
rc=$?
if [[ "${rc}" == "64" && "${out}" == *"no Rust sources"* ]]; then
  printf 'ok    a root with no Rust source exits 64\n'
  passes=$((passes + 1))
else
  printf 'FAIL  a root with no Rust source exits 64: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
