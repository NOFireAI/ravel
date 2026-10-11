#!/usr/bin/env bash
# Run Ravel's merge gates (CLAUDE.md "Gates"): fmt, clippy, tests, doctests.
#
# Usage:
#   scripts/gates.sh                 # workspace-wide -- run this once,
#                                     # right before a commit that touches
#                                     # more than one crate
#   scripts/gates.sh -p CRATE ...     # scoped: clippy/test/doctests over
#                                     # the named crates and every workspace
#                                     # crate that depends on them,
#                                     # transitively, plus the feature lanes
#                                     # that closure reaches (see below)
#   scripts/gates.sh [-p CRATE ...] --dry-run
#                                     # print the crates and the feature
#                                     # lanes the same arguments would gate,
#                                     # then exit 0 without building
#                                     # anything (scoped mode still reads
#                                     # `cargo metadata --no-deps` for the
#                                     # closure)
#
# In a dispatched fleet clone (a checkout under the fleet work root:
# /fleet/work/ or /var/lib/fleet) an unscoped run is refused with exit 64
# (ADR-2867 change 5): the executor's gate is the scoped form, and the
# PreToolUse hook covers the hand-typed workspace clippy while this covers
# the script's own. RAVEL_GATES_ALLOW_UNSCOPED=1 is the escape hatch for a
# genuinely cross-crate task; verify-dispatch-gates.sh sets it for its own
# unscoped run so a verification host under that root still produces a
# gates-pass receipt.
#
#   scripts/gates.sh --flag-doc-guard-only [FILE]
#                                     # run only the ingest-memory flag-doc
#                                     # overclaim guard, against FILE or the
#                                     # committed config.rs, and exit with its
#                                     # code (so the guard itself is testable)
set -euo pipefail

crate_args=()
dry_run=0
flag_doc_guard_only=0
flag_doc_guard_file=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p)
      shift
      if [[ $# -eq 0 ]]; then
        echo "gates.sh: -p requires a crate name" >&2
        exit 64
      fi
      crate_args+=("-p" "$1")
      shift
      ;;
    --dry-run)
      shift
      dry_run=1
      ;;
    --flag-doc-guard-only)
      shift
      flag_doc_guard_only=1
      if [[ $# -gt 0 ]]; then
        flag_doc_guard_file="$1"
        shift
      fi
      ;;
    *)
      echo "gates.sh: unknown argument: $1" >&2
      exit 64
      ;;
  esac
done

# Unscoped in a fleet clone: see the header. Judged on the script's own
# checkout, not the caller's cwd, since the gate runs on the tree it lives in.
if [[ ${#crate_args[@]} -eq 0 && ${flag_doc_guard_only} -eq 0 ]]; then
  gates_repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
  case "${gates_repo_root}" in
    */fleet/work/*|*/var/lib/fleet*)
      if [[ "${RAVEL_GATES_ALLOW_UNSCOPED:-0}" != "1" ]]; then
        echo "gates.sh: an unscoped run in a dispatched fleet clone (${gates_repo_root}) is not the executor's gate: run scripts/gates.sh -p <crate> [-p <crate2>], or set RAVEL_GATES_ALLOW_UNSCOPED=1 for a deliberate workspace gate" >&2
        exit 64
      fi
      ;;
  esac
fi

# Ingest-memory flag-doc overclaim guard (issue #1297). The two ingest memory
# flags (--max-inflight-ingest-requests, --max-ingest-buffer-bytes) once
# documented a memory bound that silently excluded the transient gzip inflate,
# so the flags claimed a ceiling they did not enforce. Each flag's own doc block
# must therefore both state what ingest memory it bounds and name the gzip
# inflate/decompression term that qualifies the claim.
#
# The check is scoped to the two flags' `///` blocks, not to config.rs as a
# whole: a file-wide scan passes on any file that mentions inflate anywhere,
# including a regression test's own doc comments, so both flag descriptions
# could lose the inflate wording while keeping their memory claim and the guard
# would still be green.
#
# Each block is flattened to one line (the `///` prefixes stripped, newlines
# turned into spaces) before matching, so a wording that wraps across two
# comment lines is still seen as one sentence. grep -q reports its own exit
# code (no pipe), per the script's own conventions.
config_rs="${flag_doc_guard_file:-$(dirname "$0")/../services/ravel-server/src/config.rs}"

# The contiguous `///` block immediately above `pub <ident>:`, flattened to a
# single line. The flag's own `#[arg(...)]` attribute lines are skipped so they
# do not break the block; any other line resets it. Exit 1 when the identifier
# is not found at all, so a renamed field fails the guard instead of emptying
# it.
flag_doc_block() {
  awk -v ident="$1" '
    /^[[:space:]]*\/\/\// {
      line = $0
      sub(/^[[:space:]]*\/\/\/[[:space:]]?/, "", line)
      block = block line " "
      next
    }
    /^[[:space:]]*#\[/ { next }
    $0 ~ "pub[[:space:]]+" ident "[[:space:]]*:" { print block; found = 1; exit }
    { block = "" }
    END { if (!found) exit 1 }
  ' "$2"
}

check_ingest_memory_flag_docs() {
  local file="$1"
  local failed=0
  local pair ident flag block tmp
  for pair in \
    "max_inflight_ingest_requests:--max-inflight-ingest-requests" \
    "max_ingest_buffer_bytes:--max-ingest-buffer-bytes"; do
    ident="${pair%%:*}"
    flag="${pair##*:}"
    block="$(flag_doc_block "${ident}" "${file}")" || block=""
    if [[ -z "${block// /}" ]]; then
      echo "gates.sh: no /// doc block found for ${flag} (${ident}) in ${file}; \
the guard cannot check a flag whose doc it cannot find (issue #1297)" >&2
      failed=1
      continue
    fi
    tmp="$(mktemp)"
    printf '%s\n' "${block}" >"${tmp}"
    if ! grep -qiE '(buffered|resident|ingest)[^.]*memory' "${tmp}"; then
      echo "gates.sh: ${flag}'s doc no longer says which ingest memory it \
bounds, so the inflate qualification below has nothing to qualify (issue \
#1297)" >&2
      failed=1
    fi
    # The term must be the gzip inflate, in one sentence with the word gzip: an
    # unqualified "decompress" matches Remote Write's own post-decompression cap,
    # which these flags mention for unrelated reasons and which would satisfy a
    # looser pattern while the gzip qualification was gone.
    if ! grep -qiE '(gzip[^.]*(inflate|decompress)|(inflate|decompress)[^.]*gzip)' "${tmp}"; then
      echo "gates.sh: ${flag}'s doc claims an ingest memory bound without \
naming the gzip inflate/decompression term (issue #1297); no sentence in \
the doc block pairs gzip with inflate or decompress" >&2
      failed=1
    fi
    rm -f "${tmp}"
  done
  return "${failed}"
}

if [[ ${flag_doc_guard_only} -eq 1 ]]; then
  guard_code=0
  check_ingest_memory_flag_docs "${config_rs}" || guard_code=$?
  exit "${guard_code}"
fi

# Scoped mode gates the reverse-dependency closure of the named crates, not
# only the crates themselves: a change to ravel-query can break ravel-sql and
# ravel-server, and a gate that never compiles them reports green on a tree CI
# rejects. The closure comes from `affected-tests.sh -n`, so the two scripts
# share one implementation and cannot disagree about what a change reaches.
gate_crates=()
gate_args=()
if [[ ${#crate_args[@]} -gt 0 ]]; then
  closure_rc=0
  closure_out="$("$(dirname "$0")/affected-tests.sh" -n "${crate_args[@]}")" || closure_rc=$?
  if [[ ${closure_rc} -ne 0 ]]; then
    echo "gates.sh: could not compute the reverse-dependency closure of ${crate_args[*]} (affected-tests.sh -n exited ${closure_rc})" >&2
    exit "${closure_rc}"
  fi
  closure_line="$(printf '%s\n' "${closure_out}" | sed -n 's/^affected crates://p')"
  read -r -a gate_crates <<<"${closure_line}"
  if [[ ${#gate_crates[@]} -eq 0 ]]; then
    echo "gates.sh: affected-tests.sh -n printed no closure for ${crate_args[*]}:" >&2
    printf '%s\n' "${closure_out}" >&2
    exit 70
  fi
  for c in "${gate_crates[@]}"; do
    gate_args+=("-p" "${c}")
  done
fi

# Feature-gated surfaces (issues #609, #616, #714, #732). ravel-server's `sql`
# and `flight-sql` features, and ravel-bench's bench-lane features, are off by
# default, so the plain clippy and test runs below compile neither the Flight
# SQL service, the bench lanes, nor their tests. The workspace run below does
# build ravel-server with `sql` on, through ravel-cli's dev-dependency, but only
# together with the
# dependency features the rest of the workspace adds: a break that shows with
# `sql` selected on its own passes it. Without these lanes a local run prints "All
# gates passed" on a tree CI rejects. That happened while rebasing #511: the
# workspace gate was green and `--features sql` failed with E0061 on a call
# site that had gone stale under a textually clean merge. It happened again for
# ravel-bench: #712 merged on a receipt but went red in CI's features job
# because gates.sh never built ravel-bench's sql-latency tests, and #724's
# `flight-lane` feature left main uncompilable under
# `--features sql-latency,profiling,flight-lane` because nothing gated it.
#
# Mirrors CI's `lint`, `flight-sql`, and `features` jobs. In scoped mode each
# lane runs when a crate it covers is in the closure computed above, so
# `-p ravel-query` runs the sql and flight-sql lanes (ravel-sql and
# ravel-server depend on it) and `-p ravel-affinity` runs none.
#
# Under --dry-run every cargo command below is printed by `run` and not
# executed, so the printed list is the list a real run executes.
run() {
  echo "==> $*"
  if [[ ${dry_run} -eq 1 ]]; then
    return 0
  fi
  "$@"
}

# The same tests as CI's `check` job, on the `ci` profile. CI runs them on
# `ci-opt`, which is `ci` with third-party dependencies at opt-level 2
# (ADR-0053, "check on ci-opt" amendment): CI restores a dependency cache
# and pays for that optimisation once, a local or fleet run builds cold and
# would pay it every time. So this run and CI's do not share artifacts, and
# a test whose result depends on how dependencies are compiled can pass
# here and fail there. To reproduce such a failure, run
# `cargo nextest run --locked --workspace --cargo-profile ci-opt`.
# Use nextest when it is installed; fall back to `cargo
# test` (which every dev machine has) otherwise. nextest cannot run
# doctests, so those always go through `cargo test --doc`, pinned to the
# `ci` profile for consistency. `command -v` in an `if` condition does not
# trip `set -e` when nextest is absent.
if command -v cargo-nextest >/dev/null 2>&1; then
  have_nextest=1
else
  have_nextest=0
fi

run_cargo_gates() {
  if [[ ${#crate_args[@]} -eq 0 ]]; then
    run cargo clippy --locked --workspace --all-targets -- -D warnings
    if [[ ${have_nextest} -eq 1 ]]; then
      run cargo nextest run --locked --workspace --cargo-profile ci
    else
      run cargo test --locked --workspace
    fi
    run cargo test --locked --doc --profile ci --workspace
  else
    echo "==> closure of ${crate_args[*]}: ${gate_crates[*]}"
    run cargo clippy --locked "${gate_args[@]}" --all-targets -- -D warnings
    if [[ ${have_nextest} -eq 1 ]]; then
      run cargo nextest run --locked "${gate_args[@]}" --cargo-profile ci
    else
      run cargo test --locked "${gate_args[@]}"
    fi
    run cargo test --locked --doc --profile ci "${gate_args[@]}"
  fi
}

run_feature_lane() {
  local feature="$1"
  shift
  run cargo clippy --locked "$@" --features "${feature}" --all-targets -- -D warnings
  if [[ ${have_nextest} -eq 1 ]]; then
    run cargo nextest run --locked "$@" --features "${feature}"
  else
    run cargo test --locked "$@" --features "${feature}"
  fi
}

want_sql=0
want_flight_sql=0
want_bench=0
want_stage_timing=0
if [[ ${#crate_args[@]} -eq 0 ]]; then
  want_sql=1
  want_flight_sql=1
  want_bench=1
  want_stage_timing=1
else
  for c in "${gate_crates[@]}"; do
    case "${c}" in
      ravel-server) want_sql=1 ;;
    esac
    case "${c}" in
      ravel-server | ravel-sql) want_flight_sql=1 ;;
    esac
    # ravel-bench's feature tests exercise these crates, so any of them in
    # scope should pay for the bench lanes.
    case "${c}" in
      ravel-bench | ravel-sql | ravel-query | ravel-ingest) want_bench=1 ;;
    esac
    # The crates that DECLARE stage-timing, and whose own cfg-gated tests the
    # ravel-bench lane below cannot reach.
    case "${c}" in
      ravel-bench | ravel-ingest | ravel-logseg) want_stage_timing=1 ;;
    esac
  done
fi

run_feature_lanes() {
  if [[ ${want_sql} -eq 1 ]]; then
    run_feature_lane sql -p ravel-server
  fi
  if [[ ${want_flight_sql} -eq 1 ]]; then
    run_feature_lane flight-sql -p ravel-server -p ravel-sql
  fi

  # ravel-bench bench lanes. The ClickBench box builds
  # `--features sql-latency,profiling` and `--features
  # sql-latency,profiling,flight-lane`; gate the widest combination so a break
  # in any of the three features (SQL corpus, profiling, Flight SQL bench) is
  # caught locally. stage-timing is a separate lane CI checks and runs, kept at
  # parity. saturation builds the ADR-1702 task 11 bins, which carry
  # `required-features = ["saturation"]`, and runs their band tests; no other
  # local gate compiles them.
  if [[ ${want_bench} -eq 1 ]]; then
    run_feature_lane sql-latency,profiling,flight-lane -p ravel-bench
    run_feature_lane saturation -p ravel-bench
    run cargo check --locked -p ravel-bench --features stage-timing --all-targets
    run cargo test --locked -p ravel-bench --features stage-timing
  fi

  # stage-timing tests in the crates that declare the feature. The ravel-bench
  # lane above enables ravel-logseg/stage-timing and ravel-ingest/stage-timing
  # transitively, so it type-checks their production code, but a `-p
  # ravel-bench` selection builds both as library dependencies and cargo never
  # compiles a dependency's `#[cfg(test)]` module. Every `#[cfg(feature =
  # "stage-timing")]` test in those two crates was therefore unreachable from
  # any lane. This runs them. Not extended to services/ravel-cli: its
  # stage-timing test is broken on main under issue #1522, and a lane running
  # it would be red for an unrelated reason.
  if [[ ${want_stage_timing} -eq 1 ]]; then
    run_feature_lane stage-timing -p ravel-logseg -p ravel-ingest
  fi
}

if [[ ${dry_run} -eq 1 ]]; then
  echo "gates.sh: dry run; the cargo commands below are printed, not run, and fmt and the source guards are skipped"
  run_cargo_gates
  run_feature_lanes
  exit 0
fi

# Linker OOM guard. On low-memory hosts (fleet executors run with 8 GB)
# the default parallelism links several multi-GB test binaries at once and
# the kernel kills ld ("signal 9"). That is a resource failure, not a code
# failure, and it wastes a full gate cycle. Cap build jobs before it
# happens. An explicit CARGO_BUILD_JOBS in the environment wins.
if [[ -z "${CARGO_BUILD_JOBS:-}" ]]; then
  mem_gb=""
  if [[ "$(uname)" == "Darwin" ]]; then
    mem_gb=$(( $(sysctl -n hw.memsize) / 1073741824 ))
  elif [[ -r /proc/meminfo ]]; then
    mem_gb=$(( $(awk '/^MemTotal/{print $2}' /proc/meminfo) / 1048576 ))
  fi
  if [[ -n "${mem_gb}" && "${mem_gb}" -le 11 ]]; then
    export CARGO_BUILD_JOBS=2
    echo "gates.sh: ${mem_gb} GB RAM; capping cargo build jobs at 2"
  fi
fi

# --locked everywhere so a local green gate warms the exact artifacts CI's
# `check` job reuses (and vice versa): CI runs against a committed
# Cargo.lock, so a divergent local resolve would recompile from scratch on
# CI and mask lockfile drift here. `cargo fmt` (rustfmt) does not accept
# --locked after the subcommand, so it takes the global-flag position.
echo "==> cargo --locked fmt --all --check"
cargo --locked fmt --all --check

# The test-hygiene shapes CLAUDE.md names under "Testing patterns": a
# wall-clock assertion, an unseeded rng, an untracked proptest seed. Each
# costs a gate rerun after the rule was already written down, so it is a
# check rather than another paragraph. A source scan, no build, so it runs
# ahead of the expensive lanes and fails at authoring time.
echo "==> scripts/guards/check-test-hygiene.sh"
"$(dirname "$0")/guards/check-test-hygiene.sh"

# Every workflow declares a top-level permissions floor (issue #1717). No local
# gate compiles .github/, so nothing else here would notice a workflow added
# without one, and the repository default workflow permission is write. Its own
# cases run first, so a guard broken into always-passing fails here rather than
# going quiet. A file scan, no build, so it sits with the other cheap guards.
echo "==> scripts/guards/check-workflow-permissions.test.sh"
bash "$(dirname "$0")/guards/check-workflow-permissions.test.sh"
echo "==> scripts/guards/check-workflow-permissions.sh"
"$(dirname "$0")/guards/check-workflow-permissions.sh"

# Operator-facing figures derived from the record-cache constants. The
# cargo tests that pin these run in CI only, and this repository's
# sessions do not run cargo locally, so four review rounds on issue #1927
# went to defects a one-second scan finds. A file scan, no build.
echo "==> scripts/guards/check-doc-figures.test.sh"
bash "$(dirname "$0")/guards/check-doc-figures.test.sh"
echo "==> scripts/guards/check-doc-figures.sh"
"$(dirname "$0")/guards/check-doc-figures.sh"

# What a `0` on the store-probe liveness gauge means had been written out
# across the probe source, its tests, the rule file and the guides. Four
# sweeps had failed to consolidate it and two had added a copy (issue #1982).
# One canonical block, every other site a pointer, and this check keeps it
# that way. A file scan, no build.
echo "==> scripts/guards/check-claim-single-source.test.sh"
bash "$(dirname "$0")/guards/check-claim-single-source.test.sh"
echo "==> scripts/guards/check-claim-single-source.sh"
"$(dirname "$0")/guards/check-claim-single-source.sh"

# An ADR amendment sometimes claims it updated other places in the same
# document to point back at it. Review kept finding the edit only reached one
# of the named places (issue #1985): this checks the claim against the
# document instead of trusting the prose. Cases first, same reason as the
# guards above: a source scan, no build.
echo "==> scripts/guards/check-amendment-integrity.test.sh"
bash "$(dirname "$0")/guards/check-amendment-integrity.test.sh"
echo "==> scripts/guards/check-amendment-integrity.sh"
"$(dirname "$0")/guards/check-amendment-integrity.sh"

# The post-fork shape guard (issue #2811) needs a pull request to check, so
# pr-review-status.sh runs it and this runs only its cases: fixture git
# repositories, no network, no build.
echo "==> scripts/guards/assert-struct-literals-current.test.sh"
bash "$(dirname "$0")/guards/assert-struct-literals-current.test.sh"

# "No wall-clock wait in an injected-clock test helper" cost two gate reruns
# (issue #1260: the flaky pair test, then its rewrite under #1235). Another
# check rather than another paragraph, same reasoning as the hygiene guard
# above: a source scan, no build, ahead of the expensive lanes.
echo "==> scripts/check-injected-clock-helpers.sh"
"$(dirname "$0")/check-injected-clock-helpers.sh"

# Format-lifecycle docs drifted from the shipped reader once already (issue
# #531). This ties each bulk-format doc's declared supported-version marker to
# its crate's SUPPORTED_VERSIONS. A source scan, no build, and it runs its own
# mismatch self-check first so it cannot go vacuous.
echo "==> scripts/check_format_version_docs.py"
python3 "$(dirname "$0")/check_format_version_docs.py"

# Every parse of caller text in ravel-sql runs the pre-parse complexity guard,
# because the walks over the parsed tree abort the process on a statement the
# parser's own recursion limit does not bound (issue #1680). That was a
# convention until issue #1760 made it a property: one guarded parse, and this
# check refuses any other. A source scan, no build, same placement reasoning as
# the two above.
echo "==> scripts/guards/check-guarded-sql-parse.sh"
"$(dirname "$0")/guards/check-guarded-sql-parse.sh"

# The same rule on the PromQL side, where an over-bound query aborts the
# process inside the parser itself (issue #529). Its funnel has a caller in
# another crate, so this one scans both crates/ravel-promql/src and
# crates/ravel-query/src (issue #1817). A source scan, no build.
echo "==> scripts/guards/check-guarded-promql-parse.sh"
"$(dirname "$0")/guards/check-guarded-promql-parse.sh"

# A parsed tenant query could reach an evaluator unreachable!() arm and abort
# the process instead of rejecting the query (issue #1701). That round
# converted every reachable arm to Error::Unsupported and left only the arms
# an exhaustive prior match already narrows out of reach, each carrying a
# one-line justification. This guard keeps new unreachable! arms honest: a
# source scan, no build, same placement reasoning as the two above.
echo "==> scripts/guards/check-promql-unreachable.sh"
"$(dirname "$0")/guards/check-promql-unreachable.sh"

# Published binaries are built on the optimised release profile (issue #2528):
# LTO and one codegen unit in [profile.release], and `cargo build --release`
# on every build line of the shipping Dockerfile. CI tunes other profiles for
# test speed; this is the one that must not follow them. Cases first, since
# the guard passes everything if its anchors move. A text scan, no build.
echo "==> scripts/guards/check-release-profile.test.sh"
bash "$(dirname "$0")/guards/check-release-profile.test.sh"
echo "==> scripts/guards/check-release-profile.sh"
"$(dirname "$0")/guards/check-release-profile.sh"

# A user guide must not keep a figure a later record superseded, or an operator
# picks a flag value from a retired number (issue #1736). This scans the guides
# tree for the retired ADR-0996 ClickBench pair. Cases first, same reason as the
# guards above: a source scan, no build, and it passes everything if its anchor
# page is gone.
echo "==> scripts/guards/check-superseded-figures.test.sh"
bash "$(dirname "$0")/guards/check-superseded-figures.test.sh"
echo "==> scripts/guards/check-superseded-figures.sh"
"$(dirname "$0")/guards/check-superseded-figures.sh"

# deny.toml's quick-xml ignore comment records the direct Cargo.lock parents
# of RUSTSEC-2026-0194/-0195 (issue #1718); this checks the comment still
# names every one of them. Cases first, same reason as the guards above. A
# Cargo.lock/deny.toml scan, no build, so it sits with the other cheap
# guards.
echo "==> scripts/guards/check-quick-xml-entry-points.test.sh"
bash "$(dirname "$0")/guards/check-quick-xml-entry-points.test.sh"
echo "==> scripts/guards/check-quick-xml-entry-points.sh"
"$(dirname "$0")/guards/check-quick-xml-entry-points.sh"

# check-quick-xml-shipped-reachability.sh itself needs a real cargo toolchain
# and dependency graph (it stays CI-only, in the supply-chain job and
# supply-chain-nightly.yml), but its own cases run against a stubbed cargo
# and cost nothing here, same reason as the entry-points cases above.
echo "==> scripts/guards/check-quick-xml-shipped-reachability.test.sh"
bash "$(dirname "$0")/guards/check-quick-xml-shipped-reachability.test.sh"

# Every shell test suite must be run by some workflow (issue #1834). Here as
# well as in CI, because the person this guard exists for is the one who just
# added a suite and has not wired it: they run gates.sh before committing, per
# the gate list in CLAUDE.md, and would otherwise learn it from a red pull
# request instead. Cases first, so a guard broken into always-passing fails
# here rather than going quiet. A git and grep scan, no build.
echo "==> scripts/tests/check-test-suites-run.test.sh"
bash "$(dirname "$0")/tests/check-test-suites-run.test.sh"
echo "==> scripts/guards/check-test-suites-run.sh"
"$(dirname "$0")/guards/check-test-suites-run.sh"

# The flag-doc overclaim guard defined above, in the ordinary gate run: a source
# scan, no build, so it fails before the expensive lanes.
echo "==> ingest-memory flag-doc overclaim guard (issue #1297)"
check_ingest_memory_flag_docs "${config_rs}"

run_cargo_gates
run_feature_lanes

# --- Gates-pass receipt ---------------------------------------------------
# Record the tree this run passed. fleet-result-merge.sh honors
# FLEET_MERGE_SKIP_GATES=1 only for a tree with a receipt here. Keyed by
# TREE hash because the merge path's authorship/squash rewrite changes
# commit ids but not content. Receipts live in the shared common dir (one
# file per tree) so worktrees see each other's runs and concurrent
# sessions never clobber each other. Written only for a full (unscoped)
# run on a clean tree; a scoped or dirty run does not prove the tree.
if [[ ${#crate_args[@]} -eq 0 && -z "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]]; then
  receipt_dir="$(cd "$(git rev-parse --git-common-dir)" && pwd)/gates-pass"
  mkdir -p "${receipt_dir}"
  gated_tree="$(git rev-parse 'HEAD^{tree}')"
  date -u +%Y-%m-%dT%H:%M:%SZ >"${receipt_dir}/${gated_tree}"
  echo "gates.sh: receipt written for tree ${gated_tree}"
fi

echo "All gates passed."
