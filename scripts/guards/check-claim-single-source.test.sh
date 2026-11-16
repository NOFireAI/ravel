#!/usr/bin/env bash
# Cases for check-claim-single-source.sh, in the pattern of
# check-doc-figures.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway tree under $TMPDIR carrying only the files the
# guard reads, so nothing here depends on the real docs or sources staying
# still. The fixture tree is clean; every case mutates one thing in it and
# asserts the exit code and a substring of the report.
#
# Run: bash scripts/guards/check-claim-single-source.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-claim-single-source.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-claim-single-source-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_tree <name>: a scratch tree that the guard reports clean. Prints its path.
new_tree() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/docs/guides" \
    "${dir}/services/ravel-server/src" "${dir}/services/ravel-server/tests" \
    "${dir}/deploy/prometheus"
  cp "${GUARD}" "${dir}/scripts/guards/check-claim-single-source.sh"

  cat >"${dir}/docs/guides/observability.md" <<'MD'
### Store reachability

| Metric | Meaning |
|---|---|
| `ravel_store_probe_last_run_timestamp_seconds` | Gauge. A reading of `0` has three causes: see [What `0` means](#what-0-means) below. |

Alert on `time() - ravel_store_probe_last_run_timestamp_seconds > 132`, for the
reasons [What `0` means](#what-0-means) below gives.

#### What `0` means

<!-- claim:store-probe-zero:canonical-begin -->
This section is the single source for what a `0` on the liveness gauge means.

**Cause 1: no probe task in this process.** `store_probe::spawn` was not called
here, so nothing stamped the gauge.

**Cause 2: the startup window.** `/metrics` begins serving before
`store_probe::spawn` runs.

**Cause 3: a pre-1970 host clock.** `SystemClock::now_ns` returns `0` through
its `unwrap_or(0)`, and a live probe re-stamps it every interval.
<!-- claim:store-probe-zero:canonical-end -->

The shipped rule is one bare staleness comparison over the gauge.

```yaml
      - alert: RavelStoreProbeStalled
        # What a 0 reading means is documented in one place: the "What 0
        # means" section of docs/guides/observability.md.
        expr: |
          time() - ravel_store_probe_last_run_timestamp_seconds > 132
```
MD

  cat >"${dir}/services/ravel-server/src/store_probe.rs" <<'RS'
/// Unix time (nanoseconds) of the last completed probe cycle or of the probe
/// task's spawn, whichever is later. For what a `0` reading means, see the
/// "What `0` means" section of docs/guides/observability.md.
static PROBE_LAST_RUN_UNIX_NS: AtomicI64 = AtomicI64::new(0);

/// The gauge source. For what a `0` reading means, see the "What `0` means"
/// section of docs/guides/observability.md.
pub fn probe_last_run_unix_ns() -> i64 {
    PROBE_LAST_RUN_UNIX_NS.load(Ordering::Relaxed)
}

fn stamp_last_run(clock: &dyn Clock) {
    // For what a `0` reading means, see the "What `0` means" section of
    // docs/guides/observability.md.
    PROBE_LAST_RUN_UNIX_NS.store(clock.now_ns(), Ordering::Relaxed);
}
RS

  cat >"${dir}/services/ravel-server/src/metrics.rs" <<'RS'
fn render_store_probe_family(out: &mut String, last_run_unix_ns: i64) {
    write_header(
        out,
        "ravel_store_probe_last_run_timestamp_seconds",
        // claim-allow: store-probe-zero -- this HELP line ships in /metrics
        // output, where the reader has no link to follow, so it carries a
        // one-line summary of the three causes rather than a pointer.
        "Unix time of the last completed cycle or of the spawn. A reading of 0 has three causes: no probe task in this process, a scrape inside the startup window before the probe is spawned, or a pre-1970 host clock, which a live probe re-stamps as 0 every interval (see the What 0 means section of docs/guides/observability.md).",
        "gauge",
    );
    write_sample_f64(out, "ravel_store_probe_last_run_timestamp_seconds", last_run_unix_ns as f64);
}

#[test]
fn renders_the_family() {
    // This renderer runs no probe. For the causes a 0 reading has in a real
    // process, see the "What `0` means" section of
    // docs/guides/observability.md.
    assert!(body.contains("ravel_store_probe_last_run_timestamp_seconds{mode=\"all\"} 0"));
}
RS

  cat >"${dir}/services/ravel-server/tests/readyz_e2e.rs" <<'RS'
/// `store_probe::spawn` stamps the liveness gauge before the loop's first
/// sleep, so a started process does not sit at its unstamped value for a whole
/// interval. The "What `0` means" section of docs/guides/observability.md
/// states what a `0` reading means.
#[tokio::test]
async fn spawn_stamps_the_gauge() {
    assert_eq!(store_probe::probe_last_run_unix_ns(), SPAWN_NS);
}
RS

  cat >"${dir}/services/ravel-server/tests/shipped_rules_name_emitted_metrics.rs" <<'RS'
/// One bare staleness comparison covers both an ageing timestamp and a `0`
/// reading (the "What `0` means" section of docs/guides/observability.md
/// states what a `0` reading means).
#[test]
fn store_probe_stalled_rule_is_one_unguarded_staleness_comparison() {
    const EXPECTED_EXPR: &str = "time() - ravel_store_probe_last_run_timestamp_seconds > 132";
}
RS

  cat >"${dir}/deploy/prometheus/ravel.rules.yaml" <<'YML'
groups:
  - name: ravel-storage-and-auth
    rules:
      - alert: RavelStoreProbeStalled
        # What a 0 reading means is documented in one place: the "What 0
        # means" section of docs/guides/observability.md.
        expr: |
          time() - ravel_store_probe_last_run_timestamp_seconds > 132
        for: 5m
YML

  cat >"${dir}/CHANGELOG.md" <<'MD'
# Changelog

## [Unreleased]

### Fixed

- **What a `0` on `ravel_store_probe_last_run_timestamp_seconds` means is
  documented in one place, and it states all three causes** (issue #1982). The
  "What `0` means" section of docs/guides/observability.md is now the one
  statement of the causes and every other site points at it.
MD
  printf '%s\n' "${dir}"
}

# check <name> <tree> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" want_rc="$3" want_sub="${4:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-claim-single-source.sh 2>&1)" || rc=$?
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

t="$(new_tree clean)"
check "a tree with one canonical block and registered pointers is clean" "${t}" 0 "clean"

# THE case this guard exists for: a tenth copy, in a file nobody registered,
# in wording that shares nothing with the other nine.
t="$(new_tree tenth-copy)"
mkdir -p "${t}/docs/guides/operations"
cat >>"${t}/docs/guides/operations/troubleshooting.md" <<'MD'
| `ravel_store_probe_last_run_timestamp_seconds` reads 0. | No probe task was
ever spawned in this process, so nothing stamped the gauge. | Check the mode. |
MD
check "a tenth copy in an unregistered file is a finding" "${t}" 1 "no-probe-task tell"

# The same, in one of the registered files, beside the gauge it is about.
t="$(new_tree copy-in-registered-file)"
sed -i.bak 's|^fn stamp_last_run|/// A pre-1970 host clock makes this store 0 forever.\nfn stamp_last_run|' \
  "${t}/services/ravel-server/src/store_probe.rs"
check "a new copy beside the gauge is a finding" "${t}" 1 "pre-1970-clock tell"

# A tell far away from any gauge mention is out of scope: unrelated prose in a
# large file must not trip this.
t="$(new_tree tell-out-of-window)"
{
  printf '\n'
  for _ in $(seq 1 30); do printf 'Filler line with nothing to do with the probe.\n'; done
  printf 'A maintenance task that was never spawned leaves its own gauge alone.\n'
} >>"${t}/CHANGELOG.md"
check "a tell outside the gauge window is not a finding" "${t}" 0 "clean"

# A pointer rewritten back into prose. The count is what catches it; a
# `contains` test over the file would still pass on the remaining two.
t="$(new_tree pointer-lost)"
sed -i.bak 's|/// "What `0` means" section of docs/guides/observability.md.|/// observability guide has more on this.|' \
  "${t}/services/ravel-server/src/store_probe.rs"
check "a pointer rewritten into prose is a finding" "${t}" 1 "expected 3"

# A new mention added to a registered site without registering it. Same rule,
# other direction, which is what catches a copy that happens to dodge the tells.
t="$(new_tree pointer-added)"
sed -i.bak 's|for: 5m|for: 5m\n        # See the "What 0 means" section of docs/guides/observability.md.|' \
  "${t}/deploy/prometheus/ravel.rules.yaml"
check "an unregistered extra pointer is a finding" "${t}" 1 "deploy/prometheus/ravel.rules.yaml"

# A second canonical block: two homes is the state this guard refuses.
t="$(new_tree two-canonical-blocks)"
cat >>"${t}/docs/guides/observability.md" <<'MD'

<!-- claim:store-probe-zero:canonical-begin -->
A second home for the same claim.
<!-- claim:store-probe-zero:canonical-end -->
MD
check "a second canonical block is a finding" "${t}" 1 "canonical begin marker"

# The canonical block moved to another file. The pointers still read as
# pointers, so only the marker's location catches this.
t="$(new_tree canonical-moved-file)"
mkdir -p "${t}/docs"
{
  printf '#### What `0` means\n\n'
  sed -n '/canonical-begin/,/canonical-end/p' "${t}/docs/guides/observability.md"
} >"${t}/docs/architecture.md"
sed -i.bak '/canonical-begin/,/canonical-end/d' "${t}/docs/guides/observability.md"
check "the canonical block moved to another file refuses" "${t}" 70 "must live in"

# Both markers deleted. A scan with no canonical block to compare against is a
# could-not-run, not a clean tree.
t="$(new_tree canonical-gone)"
sed -i.bak '/claim:store-probe-zero:canonical/d' "${t}/docs/guides/observability.md"
check "no canonical markers anywhere refuses" "${t}" 70 "were not found"

# Moved under another heading in the same file: every pointer names
# #what-0-means and would now land nowhere.
t="$(new_tree canonical-heading-moved)"
sed -i.bak 's|^#### What `0` means|#### Reading a zero|' "${t}/docs/guides/observability.md"
check "the canonical block under a different heading is a finding" "${t}" 1 "#what-0-means"

# A cause dropped from the one home. This is the original defect: nine copies
# agreeing on an explanation that was missing two of the three causes.
t="$(new_tree cause-dropped)"
sed -i.bak 's|\*\*Cause 3: a pre-1970 host clock.\*\*|**Cause 3: something else.**|' \
  "${t}/docs/guides/observability.md"
check "a cause missing from the canonical block is a finding" "${t}" 1 "a pre-1970 host clock"

# A cause stated twice in the one home is drift starting inside the home.
t="$(new_tree cause-doubled)"
sed -i.bak 's|its `unwrap_or(0)`, and a live probe re-stamps it every interval.|its `unwrap_or(0)`; a pre-1970 host clock is the only self-inflicted one.|' \
  "${t}/docs/guides/observability.md"
check "a cause stated twice in the canonical block is a finding" "${t}" 1 "2 time(s)"

# The HELP line is the one exception and has to keep naming all three, or it
# becomes the tenth copy by the one route the tells cannot see.
t="$(new_tree help-lost-a-cause)"
sed -i.bak 's|or a pre-1970 host clock, which a live probe re-stamps as 0 every interval |or something else |' \
  "${t}/services/ravel-server/src/metrics.rs"
check "the HELP line missing a cause is a finding" "${t}" 1 "HELP line names"

# The HELP line gone entirely: the exception is then checked against nothing.
t="$(new_tree help-gone)"
sed -i.bak 's|^        "Unix time of the last completed cycle.*$|        HELP_CONST,|' \
  "${t}/services/ravel-server/src/metrics.rs"
check "a missing HELP string refuses" "${t}" 70 "HELP string was not found"

# An exemption nobody registered: the escape hatch has to be as visible as the
# rule, or it becomes the way a copy lands.
t="$(new_tree unregistered-exemption)"
sed -i.bak 's|^/// task.s spawn, whichever is later. For what|/// claim-allow: store-probe-zero -- because I said so.\n/// task spawn, whichever is later. For what|' \
  "${t}/services/ravel-server/src/store_probe.rs"
check "an unregistered exemption marker is a finding" "${t}" 1 "unregistered exemption"

# An exemption with no reason suppresses nothing, so the copy under it is still
# reported and the empty reason is reported too.
t="$(new_tree reasonless-exemption)"
sed -i.bak 's|// claim-allow: store-probe-zero -- this HELP line ships in /metrics|// claim-allow: store-probe-zero -- |' \
  "${t}/services/ravel-server/src/metrics.rs"
check "an exemption with no reason is a finding" "${t}" 1 "carries no reason"

# A registered pointer site that moved. A scan over six of seven files that
# reports clean is the failure this guard exists to prevent one level up.
t="$(new_tree pointer-site-gone)"
rm "${t}/services/ravel-server/tests/readyz_e2e.rs"
check "a missing registered pointer site refuses" "${t}" 70 "not readable"

# The gauge renamed out from under the tell scan. Zero anchors means the scan
# covered nothing, which is not a pass.
t="$(new_tree gauge-renamed)"
for f in docs/guides/observability.md services/ravel-server/src/store_probe.rs \
  services/ravel-server/src/metrics.rs services/ravel-server/tests/readyz_e2e.rs \
  services/ravel-server/tests/shipped_rules_name_emitted_metrics.rs \
  deploy/prometheus/ravel.rules.yaml CHANGELOG.md; do
  sed -i.bak -e 's/ravel_store_probe_last_run_timestamp_seconds/ravel_store_probe_last_ran_seconds/g' \
    -e 's/PROBE_LAST_RUN_UNIX_NS/PROBE_LAST_RAN_NS/g' \
    -e 's/probe_last_run_unix_ns/probe_last_ran_ns/g' \
    -e 's/stamp_last_run/stamp_last_ran/g' "${t}/${f}"
done
check "the gauge renamed out of the scan refuses" "${t}" 70 "tell scan covered nothing"

t="$(new_tree bad-usage)"
out="$(cd "${t}" && bash scripts/guards/check-claim-single-source.sh extra-arg 2>&1)"; rc=$?
if [[ "${rc}" == 64 ]]; then
  printf 'ok    %s\n' "an argument is bad usage"
  passes=$((passes + 1))
else
  printf 'FAIL  %s: exit %s, wanted 64\n' "an argument is bad usage" "${rc}"
  fails=$((fails + 1))
fi

printf '\ncheck-claim-single-source.test.sh: %d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
